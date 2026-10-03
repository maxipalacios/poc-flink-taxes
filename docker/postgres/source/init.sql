-- Source PostgreSQL initialization for the tax engine PoC.
--
-- Creates the domain model consumed by the Flink CDC pipeline and seeds it
-- on first startup (the script runs only when the data directory is empty):
--
--   tax_calculations  INSERT-only tax calculation facts
--   merchants         master data, one merchant per CUIT
--
-- Re-seed from scratch with: docker compose down -v && docker compose up -d

BEGIN;

-- Mutable changelog table: inserts, updates and deletes stream into the
-- pipeline as a versioned changelog keyed by CUIT. One merchant per CUIT;
-- establishment is descriptive and never part of the key.
CREATE TABLE merchants (
    cuit          varchar(11)  PRIMARY KEY,
    name          varchar(120) NOT NULL,
    establishment varchar(30)  NOT NULL
);

-- The enrichment pipeline versions merchant rows by their change time, so
-- every UPDATE must reach the pipeline with its before image. The default
-- replica identity only ships the key columns on UPDATE, which the CDC
-- connector reports as a missing before image and fails the source; FULL
-- ships the complete old row.
ALTER TABLE merchants REPLICA IDENTITY FULL;

-- INSERT-only facts: every calculation is new; an existing one is never
-- corrected (see GLOSSARY.md, "Tax Calculation").
CREATE TABLE tax_calculations (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- synthetic key required by the CDC connector
    transaction_id uuid NOT NULL DEFAULT gen_random_uuid(),        -- one tax over one transaction; several rows may share one
    cuit           varchar(11) NOT NULL, -- no FK to merchants on purpose: missing master data must still consolidate (null enrichment)
    tax_id         varchar(30) NOT NULL, -- taxonomy: RET_* withholdings, PER_* perceptions
    tax_rate       numeric(5,2) NOT NULL, -- percent form: 3.50 means 3.5%
    base_tax       numeric(18,2) NOT NULL,
    tax_amount     numeric(18,2) NOT NULL DEFAULT 0,
    tax_status     varchar(10),  -- carried for source-model fidelity; unused by pipeline logic
    exclusion_rate numeric(5,2), -- carried for source-model fidelity; unused by pipeline logic
    created_at     timestamptz NOT NULL DEFAULT now() -- event time of the fact
);

-- DECIMAL amounts are deliberate: fiscal totals use exact decimal arithmetic.

INSERT INTO merchants (cuit, name, establishment) VALUES
    ('30692138747', 'Farmacity S.A.',              '10001'),
    ('30584620389', 'Carrefour Argentina S.A.',    '10002'),
    ('23123456783', 'Almacenes del Sur S.A.',      '10003'),
    ('27234567894', 'Distribuidora Pampeana S.A.', '10004'),
    ('30234567895', 'Insumos Textiles S.A.',       '10005');

-- Backfill of tax calculations. tax_amount always equals
-- base_tax * tax_rate / 100 so totals are easy to verify by hand.
--
-- Late-data caveat (issue #7): the consolidation pipeline drops any
-- calculation that arrives more than 5 seconds behind the newest one
-- already delivered (allowed lateness 0). The CDC snapshot's delivery
-- order is NOT the insertion order — observed scrambled across runs —
-- so the oldest backfill rows (the 26-hour ones especially) can be
-- dropped nondeterministically when a newer row is delivered before
-- them. That is the documented allowed-lateness-0 limitation, not a
-- bug: demo certificates built from the backfill may miss rows, while
-- every row inserted after the job streams consolidates. The e2e tests
-- sidestep this by deleting the backfill and planting exactly the
-- snapshot rows they assert on.
INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate, created_at)
VALUES
    -- Older backfill (about a day before initialization).
    ('30692138747', 'RET_IVA',               5.00, 40000.00, 2000.00, 'CL', 0.00, now() - interval '26 hours'),
    ('30584620389', 'RET_IIBB_CABA',         2.00, 25000.00,  500.00, 'CL', 0.00, now() - interval '26 hours'),
    -- CUIT with no merchant row: its withholdings must still consolidate later
    -- with null enrichment (missing-master-data scenario).
    ('20149543350', 'RET_IVA',               3.50,  6000.00,  210.00, 'CL', 0.00, now() - interval '26 hours'),
    ('30234567895', 'RET_GANANCIAS',         3.00, 30000.00,  900.00, 'CL', 0.00, now() - interval '20 minutes'),
    ('30234567895', 'RET_IVA',               3.50,  1000.00,   35.00, 'CL', 0.00, now() - interval '20 minutes'),
    ('23123456783', 'RET_IVA',               3.50,  5000.00,  175.00, 'CL', 0.00, now() - interval '18 minutes'),
    -- Perceptions (PER_*): present in the source model but excluded later by
    -- taxonomy — only the RET_* family consolidates into certificates.
    ('23123456783', 'PER_IIBB_CABA',         5.00,  5000.00,  250.00, 'CL', 0.00, now() - interval '18 minutes'),
    ('23123456783', 'RET_GANANCIAS',         3.00, 20000.00,  600.00, 'CL', 0.00, now() - interval '16 minutes'),
    ('30584620389', 'RET_IVA',               5.00,  8000.00,  400.00, 'CL', 0.00, now() - interval '15 minutes'),
    ('27234567894', 'RET_IVA',               5.00, 10000.00,  500.00, 'CL', 0.00, now() - interval '14 minutes'),
    ('23123456783', 'PER_IVA',              21.00,  1000.00,  210.00, 'CL', 0.00, now() - interval '14 minutes'),
    ('30584620389', 'RET_IVA',               3.50, 10000.00,  350.00, 'CL', 0.00, now() - interval '12 minutes'),
    ('30584620389', 'RET_IIBB_BUENOS_AIRES', 3.00, 10000.00,  300.00, 'CL', 0.00, now() - interval '12 minutes'),
    ('20149543350', 'RET_IVA',               3.50,  2000.00,   70.00, 'CL', 0.00, now() - interval '11 minutes'),
    ('20149543350', 'RET_GANANCIAS',         3.00,  8000.00,  240.00, 'CL', 0.00, now() - interval '11 minutes'),
    ('30692138747', 'RET_IVA',               3.50,  4000.00,  140.00, 'CL', 0.00, now() - interval '10 minutes'),
    ('27234567894', 'RET_IIBB_CABA',         2.00, 15000.00,  300.00, 'CL', 0.00, now() - interval '9 minutes'),
    ('30692138747', 'RET_IVA',               3.50, 20000.00,  700.00, 'CL', 0.00, now() - interval '7 minutes'),
    ('30584620389', 'RET_IIBB_BUENOS_AIRES', 3.00,  5000.00,  150.00, 'CL', 0.00, now() - interval '2 minutes'),
    -- Inside the current 60-second demo certification window.
    ('30692138747', 'RET_IVA',               3.50,  2000.00,   70.00, 'CL', 0.00, now() - interval '25 seconds'),
    ('30584620389', 'RET_IVA',               3.50,  1000.00,   35.00, 'CL', 0.00, now() - interval '10 seconds');

-- One transaction, several taxes: a single Farmacity transaction generates
-- three calculations sharing one transaction_id.
INSERT INTO tax_calculations (transaction_id, cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate, created_at)
SELECT tx.txid, v.cuit, v.tax_id, v.tax_rate, v.base_tax, v.tax_amount, v.tax_status, v.exclusion_rate, v.created_at
FROM (SELECT gen_random_uuid() AS txid) AS tx
CROSS JOIN (VALUES
    ('30692138747', 'RET_IVA',       3.50, 10000.00,  350.00, 'CL', 0.00, now() - interval '4 minutes'),
    ('30692138747', 'RET_IVA',       5.00, 20000.00, 1000.00, 'CL', 0.00, now() - interval '4 minutes'),
    ('30692138747', 'RET_GANANCIAS', 3.00, 50000.00, 1500.00, 'CL', 0.00, now() - interval '4 minutes')
) AS v(cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, exclusion_rate, created_at);

COMMIT;
