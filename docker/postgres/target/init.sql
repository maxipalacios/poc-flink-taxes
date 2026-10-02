-- Target PostgreSQL initialization for the tax engine PoC.
--
-- Creates the schema that materializes the certification result. Tables
-- start EMPTY: the Flink job (issues #4-#9) populates them through its
-- JDBC upsert sink. There is no REST layer; this table is the queryable
-- materialized view.

-- Naming note: the physical table name `certificate_items` follows the
-- approved design (issue #1 / issue #3 acceptance criteria). The domain
-- term for one row is "Rate Line" (GLOSSARY.md lists "certificate item" as
-- an avoid-term in prose); the physical name is kept deliberately. This
-- resolves the naming flag raised in issue #2's follow-ups.
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
