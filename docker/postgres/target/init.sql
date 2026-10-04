-- Target PostgreSQL initialization for the tax engine PoC.
--
-- Creates the schema materialized by the Flink pipeline. Tables start EMPTY
-- and are populated at runtime through JDBC sinks by the certification jobs
-- (issues #4-#9). There is no REST layer; these tables are the queryable
-- materialized view.

-- Naming note: the physical table name `certificate_items` follows the
-- approved design (issue #1 / issue #3 acceptance criteria). The domain
-- term for one row is "Rate Line" (GLOSSARY.md lists "certificate item" as
-- an avoid-term in prose); the physical name is kept deliberately. This
-- resolves the naming flag raised in issue #2's follow-ups.
--
-- The `window_start` / `window_end` columns deliberately reuse Flink's window
-- vocabulary for the certification-period boundaries: the spec fixes these
-- physical column names, and the underlying SQL is a windowed aggregation
-- over tumbling periods. In prose, prefer the domain term "certification
-- period" (GLOSSARY.md lists "window" as an avoid-term for the concept).
CREATE TABLE certificate_items (
    cuit             varchar(11)   NOT NULL,
    tax_id           varchar(30)   NOT NULL,
    window_start     timestamptz   NOT NULL, -- start of the tumbling certification period (rowtime is source `tax_calculations.created_at`); periods align to America/Argentina/Buenos_Aires
    tax_rate         numeric(5,2)  NOT NULL,
    establishment    varchar(30),            -- nullable: last merchant version seen in the period; NULL when the CUIT has no merchant (missing master data must not drop withholdings)
    merchant_name    varchar(120),           -- nullable: same enrichment semantics as establishment
    window_end       timestamptz   NOT NULL, -- certificate finality is DERIVED as window_end < now(); there is deliberately no explicit "closed" marker column
    total_base_tax   numeric(18,2) NOT NULL DEFAULT 0, -- exact DECIMAL totals by design (fiscal documents must not drift)
    total_tax_amount numeric(18,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (cuit, tax_id, window_start, tax_rate) -- upsert key: replays and retries converge idempotently without duplicates
);

-- ============================================================================
-- Pipeline role (least privilege). The `flink` bootstrap superuser stays the
-- admin account for healthchecks and manual `psql -U flink` sessions; the
-- JDBC upsert sink connects as this dedicated role instead (see README,
-- credentials). The sink emits INSERT .. ON CONFLICT DO UPDATE for every
-- consolidation result and DELETE on retractions, hence the DML grant set;
-- SELECT covers read-back verification.
-- ============================================================================
CREATE ROLE flink_sink LOGIN PASSWORD 'flink';
GRANT CONNECT ON DATABASE taxes TO flink_sink;
GRANT USAGE ON SCHEMA public TO flink_sink;
GRANT SELECT, INSERT, UPDATE, DELETE ON certificate_items TO flink_sink;
