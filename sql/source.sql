-- Source PostgreSQL helper queries.

SELECT * FROM customers ORDER BY id;

-- Inspect logical replication slots after the Flink CDC job starts.
SELECT slot_name, plugin, slot_type, active, restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots;
