-- Target PostgreSQL initialization for the tax engine PoC.
--
-- Creates the schema materialized by the Flink pipeline. Tables start EMPTY
-- and are populated at runtime through JDBC sinks: the domain tables by the
-- certification jobs (issues #4-#9), `tax_calculations_mirror` by the issue
-- #4 CDC tracer-bullet job. There is no REST layer; these tables are the
-- queryable materialized view.

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

-- ============================================================================
-- DEBUGGING ARTIFACT: raw mirror of the source `tax_calculations` table,
-- written by the Flink CDC tracer-bullet job (issue #4) to verify the CDC
-- pipeline end to end. It is not part of the domain model, nothing consumes
-- it, and later issues (#5+) do not read it. Column types mirror the source
-- table's physical schema; `id` carries over the source-generated value, so
-- the mirror declares no identity.
-- ============================================================================
CREATE TABLE tax_calculations_mirror (
    id             bigint PRIMARY KEY,
    transaction_id uuid,
    cuit           varchar(11),
    tax_id         varchar(30),
    tax_rate       numeric(5,2),
    base_tax       numeric(18,2),
    tax_amount     numeric(18,2),
    tax_status     varchar(10),
    exclusion_rate numeric(5,2),
    created_at     timestamptz
);
