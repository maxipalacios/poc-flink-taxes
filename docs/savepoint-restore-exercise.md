# Savepoint, stop, and restore exercise

Runbook for the checkpoint/savepoint recovery demo (issue #8). The goal of the issue is
that **job upgrades must avoid recomputation**: recovery has to be proven against the
running cluster, not assumed. This document records the exact commands and the real
observed outputs of one full savepoint → stop → restore cycle performed against the
`docker compose` cluster. Job ids, checkpoint numbers, and timestamps below are
genericized (`<jobId>`, `<savepoint-hash>`); everything else is what actually happened.

## Purpose

- Prove that the consolidation pipeline can be stopped with a savepoint and resumed on a
  new job submission **without re-running the CDC snapshot and without duplicating or
  losing consolidated rows**.
- Prove that changes accumulated in the source WAL *while the job was stopped* are
  delivered by the restored job (the replication slot preserved the stream position).
- Exercise the operator path you would use for planned job upgrades (deploy a new fat
  JAR, change the certification period, migrate state across job versions).

Recovery from *crashes* is a different mechanism (automatic failover from the last
completed checkpoint) and is covered by the automated e2e test described at the end of
this document.

## Prerequisites

1. The compose cluster is up (`docker compose ps` shows `jobmanager`, `taskmanager`,
   `postgres-source`, `postgres-target` healthy) and the Flink Web UI answers on
   http://localhost:8081.
2. The fat JAR is built and mounted:

   ```bash
   docker compose run --no-deps --rm dev bash -lc 'cd /workspace && gradle --no-daemon shadowJar'
   ```

   `build/libs` is bind-mounted into the JobManager at `/opt/flink/usrlib`, so no copy is
   needed. (`--no-deps` matters: without it, `compose run` also starts the postgres
   services and can race with the running cluster.)

3. No job is currently running:

   ```bash
   docker compose exec jobmanager flink list   # "No running jobs."
   ```

4. The savepoints directory lives inside the `flink-checkpoints` named volume. If it was
   just created, make it writable by the Flink user once (see
   [Troubleshooting](#troubleshooting)):

   ```bash
   docker compose exec jobmanager chown flink:flink /opt/flink/checkpoints
   ```

> **Host port 5432 note.** Every database access in this runbook uses
> `docker compose exec ... psql` against the service names, never `localhost:5432`. If
> another local service on your machine already holds port 5432, `docker compose up`
> fails to start `postgres-source` with a bind error; compose does not map
> `postgres-source` to the host in that case and `docker compose exec` is the way in.
> (`postgres-target` uses host port 5433; change it in `docker-compose.yml` if that is
> taken too.)

## Step-by-step

### 1. Submit the job

```bash
docker compose exec jobmanager \
  flink run -c com.example.taxes.TaxJob /opt/flink/usrlib/poc-flink-taxes-0.1.0-all.jar
```

The job resolves its PostgreSQL coordinates from environment variables; the defaults
resolve the compose service names and their in-network ports (target port `5432`, the
physical port of the `postgres-target` service — not the host mapping `5433`), so the
plain command above works with no extra configuration. A submission with a WRONG port
(for example `TARGET_POSTGRES_PORT=5433`, the host mapping) crash-loops the job in state
`RESTARTING` with `unable to open JDBC writer` → `Connection to postgres-target:5433
refused` (see [Troubleshooting](#troubleshooting)).

Expected client output (the job id is also visible via `flink list` or
`GET http://localhost:8081/jobs`):

```text
Job has been submitted with JobID <jobId>
```

**About the client process:** the program awaits the job result, so the `flink run`
process keeps running after printing the JobID (even with the `-d` flag) until the job
reaches a terminal state. For a streaming job that means it simply stays attached. It is
safe to interrupt it with Ctrl+C — the job keeps running on the cluster. Run the
submission in a separate terminal and keep the terminal you work in free for `psql`.

### 2. Wait for the snapshot and checkpoints

The CDC source first reads the existing `tax_calculations`/`merchants` rows (snapshot
scan), then switches to streaming. Give it 30–60 s, then verify checkpoints flow at the
10-second interval:

```bash
curl -s http://localhost:8081/jobs/<jobId>/checkpoints | python3 -m json.tool | grep -A8 counts
```

Observed counts while the job ran (checkpoint interval is 10 s):

```text
"completed": 54,
"failed": 0,
"in_progress": 0,
"restored": 0,
"total": 54
```

`completed` should keep increasing by one roughly every 10 s with `failed: 0`.

### 3. Insert clearly-labeled streaming rows

To make the demo deterministic, insert a few rows **after** the job is already streaming.
Use distinctive amounts you can grep later. `30123456781` is a CUIT with merchant master
data (Supermercados del Plata S.A.), so the rows also exercise the temporal join:

```bash
docker compose exec postgres-source psql -U flink -d taxes -c "
INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, created_at) VALUES
  ('30123456781', 'RET_IVA',        3.50, 1234.56, 43.21, 'ACTIVE', now()),
  ('30123456781', 'RET_IVA',        3.50, 2222.22, 77.78, 'ACTIVE', now()),
  ('30123456781', 'RET_GANANCIAS',  2.00,  999.99, 20.00, 'ACTIVE', now());"
```

Each row materializes into `certificate_items` once the 5 s watermark bound passes its
60 s certification window. **One caveat learned during the exercise:** the watermark only
advances when new events arrive, so the *newest* window stays open until a later row is
inserted. If nothing shows up in the target after ~1 minute, insert one more "flusher"
row (any labeled row) — its event time pushes the watermark past the earlier window and
the earlier rows consolidate immediately:

```bash
# after ~1-2 minutes, flush the open window:
docker compose exec postgres-source psql -U flink -d taxes -c "
INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, created_at)
VALUES ('30123456781', 'RET_IVA', 3.50, 555.55, 19.44, 'ACTIVE', now());"
```

Observed: the two `RET_IVA` rows consolidated into a single certificate
(`total_base_tax = 3456.78`, i.e. 1234.56 + 2222.22) with `merchant_name = Supermercados del Plata
S.A.` and `establishment = 10001`; the `RET_GANANCIAS` row produced its own row. The
flusher row itself stayed in the open window until the next event arrived — expected
behavior with allowed lateness 0.

Verify:

```bash
docker compose exec postgres-target psql -U flink -d taxes -c \
  "SELECT * FROM certificate_items ORDER BY window_start, tax_id;"
```

### 4. Capture the pre-stop state

```bash
docker compose exec postgres-target psql -U flink -d taxes -c "
SELECT cuit, tax_id, window_start, tax_rate, establishment, merchant_name,
       window_end, total_base_tax, total_tax_amount
FROM certificate_items ORDER BY 1,2,3,4;" \
  | tee pre-stop-dump.txt
```

Also record the checkpoint counts (step 2) — you will compare `restored` after the
restore. In the exercise run the dump had 5 rows.

### 5. Stop the job with a savepoint

```bash
docker compose exec jobmanager flink stop --savepointPath /opt/flink/checkpoints/savepoints <jobId>
```

**Real outcome: this worked for the pipeline as-is.** Even though the job is a pure
Table API/SQL pipeline (no custom sources that implement a stoppable interface),
`flink stop` performed a final synchronous savepoint and terminated the job cleanly:

```text
Suspending job "<jobId>" with a CANONICAL savepoint.
Triggering stop-with-savepoint for job <jobId>.
Waiting for response...
Savepoint completed. Path: file:/opt/flink/checkpoints/savepoints/savepoint-<jobId8>-<savepoint-hash>
```

Afterwards:

- `flink list` → `No running jobs.` (the job shows as `FINISHED` in `flink list -a`);
- both replication slots on the source are present but `active = f`.

So `flink stop` is the one-command flow: savepoint + graceful stop in a single step. If
`flink stop` ever refuses (it waits for a synchronous savepoint; a stalled sink can block
it), use the two-step alternative in step 6.

### 6. (Alternative) Savepoint on the running job, then cancel

If you want to keep the job running (for example to take a restore point before an
upgrade and continue), or `flink stop` does not complete, use:

```bash
docker compose exec jobmanager flink savepoint <jobId> /opt/flink/checkpoints/savepoints
```

Observed output (run against the *restored* job later in the exercise, to verify this
path too):

```text
Triggering savepoint for job <jobId>.
Waiting for response...
Savepoint completed. Path: file:/opt/flink/checkpoints/savepoints/savepoint-<jobId8>-<savepoint-hash>
You can resume your program from this savepoint with the run command.
```

Then stop the job with:

```bash
docker compose exec jobmanager flink cancel <jobId>
# Cancelling job <jobId>.
# Cancelled job <jobId>.
docker compose exec jobmanager flink list   # "No running jobs."
```

Both jobs stopped this way leave the source's replication slots in place (inactive), so
the next submission — restore or not — resumes from the slot position.

### 7. Insert rows while the job is stopped

```bash
docker compose exec postgres-source psql -U flink -d taxes -c "
INSERT INTO tax_calculations (cuit, tax_id, tax_rate, base_tax, tax_amount, tax_status, created_at) VALUES
  ('30123456781', 'RET_IVA',       3.50, 4444.44, 155.55, 'ACTIVE', now()),
  ('30123456781', 'RET_GANANCIAS', 2.00, 3333.33,  66.66, 'ACTIVE', now());"
```

Nothing consumes them yet: they accumulate in the source WAL behind the replication
slots. Verify the target is untouched (`psql ... SELECT count(*) FROM certificate_items;`
still returns the pre-stop count).

### 8. Restore the job from the savepoint

```bash
docker compose exec jobmanager \
  flink run -s /opt/flink/checkpoints/savepoints/savepoint-<jobId8>-<savepoint-hash> \
  -c com.example.taxes.TaxJob /opt/flink/usrlib/poc-flink-taxes-0.1.0-all.jar
```

- Try it **without** `-n` / `--allowNonRestoredState` first. In the exercise the restore
  completed without it — the savepoint claimed state for every operator, and the client
  printed `Job has been submitted with JobID <newJobId>`. Only add `-n` if the restore
  explicitly reports unclaimed state (for example after removing an operator from the
  job graph); note that `-n` silently skips that state, so treat it as a last resort.
- Restore with the **same connection environment as the original submission**. The JDBC
  URLs are baked into the job graph from the submission environment, not from the
  savepoint; a restore submitted with different coordinates fails the same way a fresh
  submission with those coordinates would.

JobManager log evidence that the new job restored from the savepoint (genericized):

```text
INFO  CheckpointCoordinator  - Starting job <newJobId> from savepoint /opt/flink/checkpoints/savepoints/savepoint-<jobId8>-<savepoint-hash> ()
INFO  CheckpointCoordinator  - Restoring job <newJobId> from Savepoint 56 @ 0 for <newJobId> located at file:/opt/flink/checkpoints/savepoints/savepoint-<jobId8>-<savepoint-hash>.
INFO  CheckpointCoordinator  - No master state to restore
INFO  SourceCoordinator      - Restoring SplitEnumerator of source Source: tax_calculations_cdc[1] from checkpoint.
```

`GET http://localhost:8081/jobs/<newJobId>/checkpoints` now reports `"restored": 1`, and
the checkpoint id numbering continues where the stopped job left off (the savepoint was
checkpoint 56; the first completed checkpoint of the new job is 57).

### 9. Verify convergence, no duplicates, no recomputation

Run all four checks:

1. **Pre-stop rows are intact.** Repeat the dump from step 4 and compare. Observed: all
   5 pre-stop rows byte-identical, no duplicates. (The `certificate_items` primary key
   `(cuit, tax_id, window_start, tax_rate)` means upserts overwrite rather than
   duplicate, so the dump comparison is the real proof.)
2. **The WAL backlog is delivered.** The rows inserted during the stop materialize once
   the restored job's stream catches up and the watermark closes their window (insert a
   flusher row per step 3 if they are the newest events). Observed: the two stop-window
   rows consolidated with exactly the inserted amounts (`4444.44` / `3333.33`), proving
   the replication slots carried the stream position across the stop.
3. **No snapshot recomputation.** TaskManager log right after the restore (genericized):

   ```text
   INFO  IncrementalSourceReader  - Source reader 0 received the stream split : StreamSplit{splitId='stream-split', offset=Offset{lsn=LSN{0/197a080}, txId=829, ...}, isSnapshotCompleted=false}
   INFO  PostgresConnectorTask    - Found previous offset PostgresOffsetContext [... lsn=LSN{0/1977bb8}, txId=824, snapshot=FALSE ...]
   INFO  InitialSnapshotter       - Previous snapshot has completed successfully, streaming logical changes from last known position
   INFO  PostgresSnapshotChangeEventSource - According to the connector configuration no snapshot will be executed
   INFO  ChangeEventSourceCoordinator - Snapshot ended with SnapshotResult [status=SKIPPED, ...]
   INFO  PostgresStreamFetchTask  - Execute StreamSplitReadTask for split: StreamSplit{splitId='stream-split', ...}
   ```

   and the periodic per-checkpoint lines stay in streaming mode for the lifetime of the
   job:

   ```text
   INFO  IncrementalSourceReader  - Stream split offset on checkpoint 99: Offset{lsn=LSN{0/197b5e8}, txId=831, ...}
   ```

   The reader resumes the *same* stream split from the saved LSN; there is no snapshot
   split enumeration and no chunk scanning. Seeded rows are not re-read, and their
   already-consolidated certificates are not touched.
4. **Checkpoint cadence continues.** `restored: 1`, `failed: 0`, `completed` climbing
   every ~10 s.

### 10. Cleanup

```bash
docker compose exec jobmanager flink cancel <newJobId>
docker compose exec jobmanager flink list   # "No running jobs."
```

Demo rows in `tax_calculations` and consolidated rows in `certificate_items` may stay —
only the seed data must remain untouched (it is; the source had the 24 seeded
calculations plus the demo inserts). See [Replication slot lifecycle](#replication-slot-lifecycle)
below for what the stopped job leaves behind.

## Success criteria

The savepoint → stop → restore cycle is proven when:

- [ ] `flink stop` (or `flink savepoint` + `flink cancel`) completes with a savepoint
      path, and `flink list` shows no running jobs.
- [ ] The restore submission succeeds **without** `--allowNonRestoredState`, and the
      JobManager logs show `Starting job ... from savepoint` / `Restoring job ... from
      Savepoint` for the new job id.
- [ ] `GET /jobs/<newJobId>/checkpoints` reports `"restored": 1` and checkpoints keep
      completing every ~10 s with `failed: 0`.
- [ ] The post-restore dump of `certificate_items` contains every pre-stop row
      unchanged, with no duplicate rows.
- [ ] Rows inserted while the job was stopped materialize after the restore (WAL
      backlog delivered through the preserved replication slots).
- [ ] The restored job logs `no snapshot will be executed` / `SnapshotResult
      [status=SKIPPED]` and resumes the `stream-split` from the saved LSN — no snapshot
      recomputation.

## What `flink stop` actually did

The exercise expected pure-SQL/Table API pipelines to be *not* stoppable, but on this
stack (Flink 1.20.5, PostgreSQL CDC connector) `flink stop --savepointPath` succeeded:
it triggered a **synchronous savepoint** (all tasks acknowledge; the source flushes its
last stream position into the savepoint), wrote it as a *canonical* savepoint, and moved
the job to `FINISHED`. Observed effects:

- the savepoint is a regular directory under
  `/opt/flink/checkpoints/savepoints/savepoint-<jobId8>-<savepoint-hash>` (`_metadata`
  plus the state files) inside the `flink-checkpoints` volume, so it survives container
  restarts;
- both replication slots were released (`active = f`) but kept, so the restored job
  resumes streaming instead of re-snapshotting;
- the job cannot be "resumed in place": a restore is always a **new** job id via
  `flink run -s ...`, which is exactly what a planned upgrade does anyway.

## Troubleshooting

- **`Failed to create directory for shared state: file:/opt/flink/checkpoints/<hash>/shared`**
  at submission (job fails instantly with `Could not start the JobMaster`). The
  `flink-checkpoints` volume was created by root. One-time fix:
  `docker compose exec jobmanager chown flink:flink /opt/flink/checkpoints`.

- **Job stays in `RESTARTING`, exceptions show `unable to open JDBC writer` →
  `Connection to postgres-target:5433 refused`.** The submission (or restore) was made
  with a wrong `TARGET_POSTGRES_PORT` (`5433` is the compose HOST mapping; the
  in-network port the job must use is `5432`, which is the default). Cancel the job
  (`flink cancel <jobId>`), verify with
  `docker compose exec jobmanager flink list`, and resubmit without the override (or
  with the correct one). Check the real cause with
  `curl -s http://localhost:8081/jobs/<jobId>/exceptions`.

- **Replication slot errors on resubmission after an unclean stop.** If a previous job
  died while the CDC reader was still connected (for example the TaskManager container
  was killed), the slot can remain `active = t` for a while and a new submission fails
  claiming the slot is in use. Check
  `SELECT slot_name, active FROM pg_replication_slots;` on `postgres-source`, wait for
  the old connection to time out (or restart `postgres-source`), and only as a last
  resort drop the slot manually — see below. Never drop a slot that is still `active`.

- **Rows inserted but nothing consolidates.** Not a failure: the watermark only advances
  on new events, and allowed lateness is 0, so the newest 60 s window stays open. Insert
  another row a minute later (the flusher pattern in step 3). The same applies right
  after a restore — the stop-window rows appear once a later event pushes the watermark
  past their window end.

- **`ClassNotFoundException` / stale jar after `gradle clean`.** `build/libs` is a bind
  mount taken when the containers started. After rebuilding, recreate the Flink
  containers so the mount is refreshed:
  `docker compose up -d --force-recreate jobmanager taskmanager`.

- **`postgres-source` unreachable from the host on port 5432.** Another local service
  may already own 5432; compose then cannot bind the port (see the note in
  Prerequisites). Always use `docker compose exec postgres-source psql ...` in this
  runbook, which works regardless of host port bindings.

- **The `flink run` client "hangs" after submitting.** Expected: the program awaits the
  streaming job's result, so the client process stays attached (even with `-d`) until
  the job ends. Interrupting it (Ctrl+C) does not affect the running job.

## Replication slot lifecycle

The CDC sources create one logical replication slot per table on `postgres-source`,
named `flink_tax_certificates` (tax_calculations) and `flink_tax_certificates_merchants`
(merchants). Observed behavior across the exercise:

- Slots are created on first submission and **persist across stop/restore and even
  `flink cancel`** — `SELECT slot_name, active FROM pg_replication_slots;` shows both
  slots with `active = f` once no job is running.
- A resubmission or a restore from savepoint **reuses the same slots** (the names are
  part of the job's configuration), which is what allows the restored job to resume the
  WAL exactly where the savepoint left off.
- While a slot exists and no one consumes it, PostgreSQL retains the WAL it needs —
  leaving the slots behind while iterating is safe short-term, but do not leave a dead
  slot forever on a long-lived database (check `restart_lsn` / disk usage).
- Manual drop (only while no job runs, otherwise the next submission re-creates the
  slots and re-snapshots from scratch):

  ```bash
  docker compose exec postgres-source psql -U flink -d taxes -c \
    "SELECT pg_drop_replication_slot('flink_tax_certificates');"
  docker compose exec postgres-source psql -U flink -d taxes -c \
    "SELECT pg_drop_replication_slot('flink_tax_certificates_merchants');"
  ```

  Dropping a slot discards the stream position: the next job start performs a full CDC
  snapshot again (and, with allowed lateness 0, most seeded rows will then be dropped
  per the documented late-arrival caveat in `docker/postgres/source/init.sql`).

## Cleanup and reset

- Normal end state: `flink list` shows no running jobs; demo rows may stay; slots stay
  (inactive).
- Optional: drop both slots as shown above (they will be recreated — and a fresh
  snapshot taken — on the next submission).
- Full reset, destroying volumes and re-seeding both databases (described here for
  completeness — this deletes the savepoints and checkpoints too, so take any savepoint
  you need *out* of the volume first if you want to keep it):

  ```bash
  docker compose down -v
  docker compose up -d
  docker compose exec jobmanager chown flink:flink /opt/flink/checkpoints
  ```

## Automated checkpoint-recovery test

The savepoint exercise above is manual and demo-oriented. Crash recovery is exercised
automatically by `CheckpointRecoveryE2eTest` (source tree, Testcontainers-based like the
other e2e tests): a mid-run task failure triggers the job's restart-strategy failover,
which restores every operator from the last completed checkpoint, and the test asserts
the target converges without duplicate or missing rate lines.

How the two exercises differ:

| | Checkpoint failover (automated test) | Savepoint → stop → restore (this runbook) |
| --- | --- | --- |
| Trigger | unexpected task failure | planned stop (upgrade/migration) |
| Recovery source | last completed checkpoint (automatic) | explicitly taken savepoint |
| Job identity | same job id, restarted in place | new job id via `flink run -s` |
| What you must do | nothing — restart strategy handles it | take savepoint, resubmit |
| Use it for | confidence that crashes lose no data | job upgrades, config changes, moving the job |
