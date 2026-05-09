# PgSeries — managed-PG-compatible time-series layer

## Problem

TimescaleDB delivers hypertables, continuous aggregates, retention, and
columnar compression — but ships as a C extension requiring
`shared_preload_libraries`, so it's unavailable on most managed Postgres
(RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon, Crunchy Bridge).

## Goal

A pure-SQL/PL-pgSQL time-series layer in the PgQue style: single-file
install (`\i pgseries.sql`), no C code of our own, no
`shared_preload_libraries`, no restart. Two layers:

- **pgseries-core:** chunk lifecycle, retention, rollup engine,
  compression pipeline.
- **pgseries-api:** `create_series_table()`, `add_continuous_aggregate()`,
  `add_retention_policy()`, `add_compression_policy()`,
  `add_recompression_policy()`, `alter_job()`, `pause_job()`,
  `resume_job()`.

PostgreSQL 15+ (PG 14 lacks `lz4` as default TOAST compression and
`MERGE`; PG 17's `MERGE/SPLIT PARTITION` used opportunistically).
`track_commit_timestamp = on` is required for the cagg watermark; the
installer checks and errors loudly if it is off.

## Privileges

PgSeries needs no `shared_preload_libraries` of its own, but the
recommended scheduler is `pg_cron` (which does); a cron-free fallback
exposes `pgseries.run_due_jobs()` as a SQL function callable from any
external scheduler.

## Naming

- `pgseries` — schema, package, repo, CLI, function prefix
- `PgSeries` — prose, headings, README

## TimescaleDB feature parity

What you get from PgSeries vs TimescaleDB. Implementation details live
in *In scope (v0.1)* below.

### Supported in v0.1 (clean fit)

- **Hypertables** (as "series tables") — automatic time-based chunking
  on native PG range partitioning.
- **Retention policies** — scheduled drop of old chunks.
- **Compression policies** — scheduled roll of cold chunks into a
  columnar sibling. **Recompression** is a separate, independently
  scheduled policy.
- **Columnar compression** with explicit **segmentby** (scalar columns
  for tag filtering) and **orderby** (parallel arrays for time/value
  columns), frame-of-reference for ints/timestamps, per-column null
  bitmaps, dictionary encoding for low-cardinality text, TOAST/lz4 as
  the final stage. Target 3–6×.
- **Decompression** — explicit API for write-path access to compressed
  ranges, plus an opt-in **staging sibling** for late writes (no full
  decompress on every write).
- **Continuous aggregates** — incrementally-maintained rollup tables
  with an always-on invalidation log; refresh policy defines the
  reconciliation window (`refresh_lag` / `start_offset` / `end_offset`).
  Watermark anchored on `pg_snapshot_xmin()` resolved to wall-clock via
  `pg_xact_commit_timestamp()`.
- **Real-time aggregation** — cagg view unions the materialized rollup
  with on-the-fly aggregation of source rows past the materialization
  watermark. v0.1 forbids `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`,
  and window functions in cagg select lists; see §3.
- **Finalized vs partials cagg storage** — finalized by default;
  `partials` opt-in for hierarchical caggs.
- **Hierarchical caggs (cagg-on-cagg)** — fixed-width buckets only in
  v0.1 (seconds … weeks). `month`/`year` excluded because they are not
  integer multiples of seconds.
- **Reorder / clustering policies** — `CLUSTER` on chunks via cron.
- **`time_bucket()`** — pure SQL, fixed-width parity.
- **`time_bucket_gapfill()`** — pure SQL, single-group-key in v0.1.
- **`first()` / `last()` aggregates** — composite-state custom aggregates;
  `DISTINCT ON` recommended for hot paths. `orderby DESC` is supported on
  the compressed sibling so last-point queries can early-exit.
- **Chunk exclusion / data skipping** — partition pruning on the
  compressed sibling plus BRIN `minmax_multi_ops` over sidecar
  `seg_min_ts` / `seg_max_ts` and segmentby columns; the read view uses
  `LATERAL` after a filtered subquery so quals land on the BRIN scan
  before any unnest.
- **Job scheduler** — `pg_cron`, plus `pgseries.run_due_jobs()` for any
  external driver.
- **Job control API** — `alter_job`, `pause_job`, `resume_job`,
  `run_job_now`, retry-on-failure with exponential backoff.
- **Information views** — `pgseries_information.jobs`, `job_stats`,
  `job_errors`, `chunks`, `compression_settings`,
  `continuous_aggregates`, `compressed_chunk_stats`, `cagg_lag`.
- **`ts_insert_blocker` analog** — write-path enforcement on
  compressed chunks (error by default; opt-in staging-write or
  full-decompress).
- **`show_chunks` / `drop_chunks`** — operational APIs.
- **Migration** — `pgseries.adopt_partitioned_table(regclass)` adopts
  an existing range-partitioned table in place; preferred Timescale
  migration path is per-chunk parallel `pg_dump` (Timescale chunks are
  individually dumpable), not a single `COPY`.
- **Roles** — `pgseries_reader` / `pgseries_writer` / `pgseries_admin`.

### Partial or lossy

- **Hyperfunctions** (`percentile_agg`, `histogram`, `counter_agg`,
  `time_weight`, ASOF, `locf`, `interpolate`) — slow in PL/pgSQL; lean
  on PG built-ins (`percentile_cont`, `width_bucket`) and `DISTINCT ON`.
- **Approximation sketches** — `hll` supported (broadly available on
  managed PG); `tdigest` dropped from the managed-PG happy path
  (unavailable on RDS/Aurora); pure-SQL t-digest is v0.2-conditional.
- **Multi-group `time_bucket_gapfill`** — v0.1 single-group-key only.
- **Lossless float compression** — TimescaleDB uses Gorilla; v0.1 ships
  only opt-in lossy requantization. Lossless float compression awaits
  a viable pure-SQL codec.
- **Cagg select-list shapes** — `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`,
  and window functions are rejected by `add_continuous_aggregate()` in
  v0.1; see §3.

### Out of scope (infeasible without C)

- **Bit-level codecs** — Gorilla, delta-delta, simple-8b. PL/pgSQL
  per-value overhead inverts the economics.
- **Custom planner and executor nodes** — vectorized compressed scan,
  chunk-exclusion planner hooks, constraint-aware parallel append.
  Replaced (imperfectly) by partition pruning + BRIN + view-layer
  predicate pushdown via `LATERAL`.
- **Distributed hypertables** — deprecated upstream as of TimescaleDB 2.14.
- **Tiered storage to object storage** — Timescale Cloud-only, requires
  custom storage hooks.
- **Promscale-style ingest caggs** — requires the bypassed insert path.
- **Space partitioning (`add_dimension`)** — not needed for the v0.1
  cardinality target; see *Cardinality* below.

## In scope (v0.1)

### 1. Series tables

Native PG range partitioning by time. Chunks are pre-created by a cron
job ahead of the write frontier. Chunk creation does **not** rely on
advisory locks on the parent OID — that pattern deadlocks against
retention (writer holds advisory, retention waits in
`AccessExclusive` on the same parent). Instead:

- A `pgseries.chunk_lease` table holds one row per pending chunk
  range. The chunk-creator job claims a row with
  `select … from pgseries.chunk_lease where state = 'pending' for
  update skip locked`, creates the partition, marks `state = 'ready'`,
  commits.
- The `BEFORE INSERT` trigger is the rare fallback when a write outruns
  the pre-creator. It claims a lease row the same way; if no row exists
  it inserts one and proceeds. Hot path overhead: one indexed
  `chunk_lease` lookup per insert that misses the pre-created range.
- Retention reads the same lease table and refuses to drop ranges
  with `state in ('compressing', 'leased_for_write')`.

`SECURITY DEFINER` reconciliation: pre-creator and trigger functions
are owned by `pgseries_admin`, so created chunks inherit
`pgseries_admin` ownership. Writers get table-level grants via the
`pgseries_writer` role membership on the parent. No writer ever owns
a chunk; ownership is uniform across all chunks of a series table.

Catalog tables: `pgseries.series_table`, `pgseries.chunk`,
`pgseries.chunk_lease`, `pgseries.dimension`.

### 2. Retention policies

`pg_cron` job calls `pgseries.drop_chunks(table, before := now() - N)`.
Chunks under active compression or with `state = 'leased_for_write'`
in `pgseries.chunk_lease` cannot be dropped concurrently.

### 3. Continuous aggregates

Incrementally-maintained rollup tables. An **`AFTER STATEMENT` trigger
with transition tables** on the source table appends one
`(min_time, max_time, xmin)` row per statement to
`pgseries.cagg_invalidation`, where `xmin = pg_current_xact_id()`.
`AFTER ROW` triggers are not used: they fire per-row and serialize
`COPY`/bulk insert into the queue's main bottleneck.

**Invalidation log coalescing.** A maintenance job runs every
`coalesce_interval` (default 5 s) and merges adjacent or overlapping
ranges per `(series_table, cagg)` into a single row. Without this,
a hot row produces N entries and refresh degenerates to O(N) work
per refresh tick.

**Materialization watermark.** v0.1 requires
`track_commit_timestamp = on`. The watermark is computed once per
refresh as

```
watermark_xid  := pg_snapshot_xmin(pg_current_snapshot())
watermark_time := pg_xact_commit_timestamp(watermark_xid)
                  - safety_margin    -- default 1 s
```

`safety_margin` covers clock skew on commit-timestamp resolution.
Refreshes only consume invalidation rows whose `xmin < watermark_xid`,
so in-flight transactions are never skipped (the PgQ tick pattern).
The cagg view's "real-time" half reads source rows with
`time >= watermark_time`. Storing `xmin` per invalidation row makes
this independent of clock skew on the commit-timestamp side; the
xid8-comparison is the source of truth, with the timestamp only used
to constrain the source-side scan.

**Real-time aggregation, straddle-bucket rule.** A bucket B that
crosses the watermark gets contributions from both halves. v0.1
guarantees correct counts and sums by:

- materialized half: never writes a partial bucket — refresh always
  closes a bucket only when `bucket_end <= watermark_time`.
- real-time half: aggregates source rows with `time >= watermark_time`,
  which by construction lies inside an open bucket.

The cagg view returns `union all` of both halves with a single outer
`group by bucket`. v0.1 rejects:

- `HAVING` clauses
- `GROUPING SETS` / `ROLLUP` / `CUBE`
- window functions in the cagg select list

These shapes break the union-then-regroup pattern. Wrap the cagg view
in a regular view to apply them at query time. v0.2 may lift the
restriction once recombination semantics are specified per shape.

**Hierarchical caggs (cagg-on-cagg)** are supported only when both
parent and child use **fixed-width buckets** (microsecond … week),
child width is an integer multiple of parent's, and both align on a
common epoch. `month` and `year` buckets are not fixed-width and are
forbidden in cagg-on-cagg in v0.1.

### 4. Columnar compression (pure SQL)

Cold chunks roll into a sibling `_compressed` table that is itself
partitioned with the same time bounds, so partition pruning fires
before the read view unnests. Each compressed row stores N source
rows with explicit **segmentby** and **orderby**:

- **segmentby columns** (e.g. `device_id`) — scalar on the compressed
  row. Predicates on segmentby skip whole compressed rows without
  unnesting. Single most important correctness/performance feature;
  not the same as sort order.
- **orderby columns** (typically `time`) — parallel typed arrays,
  sorted within each segment. **`orderby DESC`** is supported per
  column at compression time so last-point (`order by time desc limit 1`)
  queries can stop after one segment.
- **value columns** — parallel typed arrays.

**Segmentby cardinality.** Compression ratio collapses when segmentby
cardinality approaches segments-per-chunk. With ~1 day chunks at typical
metric rates, segmentby cardinality should stay below ~10⁴ per chunk.
Above that, segments hold few rows, FOR encoding loses to overhead, and
ratio drops to ~1.5×. `add_compression_policy()` warns when the chunk's
distinct segmentby count exceeds `segmentby_warn_threshold` (default
50000); the install README documents this prominently.

Pipeline (vectorized SQL):

- group by segmentby, sort by orderby
- frame-of-reference for ints/timestamps: `min(col)` + `int2[]`/`int4[]`
  offsets, fall back to `int8[]` when range overflows
- per-column null bitmap (`bytea`)
- dictionary encoding for low-cardinality text (per-chunk `text[]` dict
  + `int2[]` indices)
- optional float requantization (lossy, opt-in per column; lossless FOR
  is integer/timestamp only)
- segment row count tuned so packed arrays land in TOAST out-of-line
  storage (above the toast threshold, ~2 KiB) where `lz4` actually runs.
  Per-column **`storage extended`** is set explicitly. Earlier drafts of
  this spec said `storage external`, which is wrong: `external` stores
  out-of-line uncompressed, so `lz4` never fires. `extended` is the
  correct, default storage class and is what the pipeline targets.
- **Read-only enforcement** uses a `BEFORE INSERT OR UPDATE OR DELETE
  OR TRUNCATE` trigger on each compressed chunk that raises
  `read_only_violation`. `CHECK` constraints constrain row values, not
  writes, and cannot enforce read-onlyness. The trigger is paired with
  an explicit `revoke insert, update, delete, truncate on <chunk> from
  pgseries_writer` so the error path is never reached in normal
  operation; the trigger is the safety net for misconfigured grants.

**Compression swap.** `DETACH PARTITION CONCURRENTLY` followed by
re-`ATTACH` is *not* used — it is a two-phase operation that can leave
the partition in `pg_partitioned_table.partattrs = …` with
`partpending` semantics and exposes a window where mid-swap readers
see neither the old nor the new partition for the same range. Instead,
the swap takes a short `AccessExclusive` lock on the parent under
`SET LOCAL lock_timeout = '1s'`, performs `DETACH … FINALIZE` of the
heap partition and `ATTACH PARTITION` of the compressed sibling in a
single transaction, then commits. If the lock cannot be acquired,
the job retries on the next tick with truncated exponential backoff;
no state is left half-applied. Plan-cache invalidation is unavoidable
and is documented.

**Post-swap BRIN.** `autosummarize=on` is asynchronous (driven by
autovacuum), so the first range queries after a compression swap
do not benefit from BRIN skipping. The compression job calls
`select brin_summarize_new_values(<index>)` synchronously after the
swap so the compressed chunk's first read already skips correctly.

Target ratio: **3–6×** against heap + TOAST (excluding indexes) on
integer/timestamp telemetry; lower on floats without bit-packed codecs.
Benchmarks report per-column-type breakdown.

### 5. Write path on compressed chunks

Three modes, picked per series table:

1. **`error`** (default): a `BEFORE INSERT OR UPDATE OR DELETE` trigger on
   the compressed sibling raises with a message pointing at the
   decompression API.
2. **`staging`** (recommended for late writes): writes go to a sibling
   `_staging` heap partition that shares the same time range.
   `add_recompression_policy()` periodically merges staging rows into
   the compressed sibling. Reads union all three (uncompressed,
   staging, compressed) via the read view in §6. This avoids full
   decompress on every late write — the model TimescaleDB 2.11+ adopted.
3. **`auto_decompress`**: opt-in. Decompresses affected segments back
   into the uncompressed sibling, applies the write, marks for
   recompression by the recompression policy.

**Concurrent decompress + write.** Staging and `auto_decompress` modes
take a per-`(chunk_id, segment_hash)` advisory lock
(`pg_advisory_xact_lock(chunk_id, segment_hash)`) so two writers cannot
race to decompress and re-compress the same segment. Lease lookup uses
`for update skip locked` against `pgseries.chunk_lease` for chunk-level
state, advisory lock for segment-level state.

**`ON CONFLICT`** on compressed chunks errors in all three modes — there
is no per-row primary key in compressed form. Note (PG general rule,
documented prominently): `ON CONFLICT` on a partitioned series table
requires the unique index to include the partition key column (`time`).

### 6. Transparent compressed reads

A read view unions the uncompressed, staging, and compressed siblings.
Predicate pushdown is achieved with `LATERAL` after a filtered subquery,
not by direct `unnest` in the select list. The shape is:

```sql
create view <table> as
  select * from <table>_uncompressed
  union all
  select * from <table>_staging
  union all
  select c.unpacked.*
  from (
    select compressed_row
    from <table>_compressed
    where seg_max_ts >= $a and seg_min_ts <= $b
      -- segmentby predicates also pushed here
  ) f,
  lateral pgseries.unpack_segment(f.compressed_row) as c(unpacked);
```

The filtered subquery lets the planner land time and segmentby quals on
the BRIN/partition scan **before** the lateral unnest fires. A naive
`select unnest(...)` in the outer select cannot push filters through.

**Plan-shape acceptance test (CI).** A pgTAP regression test runs
`EXPLAIN (FORMAT JSON, BUFFERS)` on a representative range query and
asserts:

- partition pruning on `<table>_compressed` is applied
- the chosen index is BRIN (`Index Cond` referencing `seg_min_ts`/`seg_max_ts`)
- no `Function Scan` on `unnest`/`unpack_segment` appears for skipped
  partitions
- segmentby predicates appear in the inner filter, not as a post-unnest
  filter

The test runs on PG 15/16/17/18 in CI and is the load-bearing
correctness signal for the read path.

### 7. Data skipping without planner hooks

Sidecar stats columns on every compressed row: `seg_min_ts`,
`seg_max_ts`, `seg_row_count`, per-column null counts, segmentby
scalars. BRIN index using **`minmax_multi_ops`** (PG 14+) over
`(seg_min_ts, seg_max_ts)` and over each segmentby column.
`minmax_multi_ops` tolerates non-correlated insertion order, which is
exactly what compressed rows produce when segments arrive grouped by
segmentby rather than time. Plain `minmax_ops` would produce useless
ranges in this layout.

`pages_per_range` defaults to 32; `add_compression_policy()` accepts
an override. Compressed tables are append-only after creation;
re-`CLUSTER` invalidates BRIN correlation (documented).
`autosummarize=on`, plus the synchronous
`brin_summarize_new_values()` call from §4 post-swap.

### 8. Time helpers

`time_bucket()` and `time_bucket_gapfill()` in pure SQL
(`generate_series` + left join). Fixed-width buckets (microsecond …
week) in v0.1; `month` and `year` are accepted by `time_bucket()` but
disallowed in cagg-on-cagg parents (§3). Gapfill is single-group-key
in v0.1; multi-group + `locf`/`interpolate` deferred.

### 9. Reorder / clustering policies

`CLUSTER` on chunks via cron, only on chunks not under active
compression (lease check against `pgseries.chunk_lease`).

### 10. Aggregates

`first(value, time)` and `last(value, time)` as pure-SQL custom
aggregates with composite `(value, time)` state. `PARALLEL SAFE` only
after combinefunc verification. Convenience, not performance — use
`DISTINCT ON` on hot paths. Last-point queries on compressed data
benefit from `orderby DESC` (§4) so the unpack stops after one segment.

### 11. Operational surface

`pgseries_information` schema with views matching TimescaleDB naming
where reasonable: `jobs`, `job_stats`, `job_errors`, `chunks`,
`compression_settings`, `continuous_aggregates`,
`compressed_chunk_stats`, `cagg_lag`. `cagg_lag` reports per-cagg
`(materialization_watermark, latest_invalidation_time, lag_seconds,
out_of_window_writes)` and is the surface for the acceptance criterion
on out-of-window writes.

Job control: `alter_job`, `pause_job`, `resume_job`, `run_job_now`,
retry-on-failure with exponential backoff.

**Cron-free fallback.** When `pg_cron` is unavailable:

```sql
select pgseries.run_due_jobs(
  max_jobs       := 100,    -- upper bound on jobs claimed this call
  lease_seconds  := 60,     -- soft deadline for in-flight jobs
  block          := false   -- false = return immediately when nothing due
);
```

Each invocation:

1. Claims pending jobs with `select … from pgseries.jobs where
   next_run_at <= now() and not running for update skip locked
   limit max_jobs`.
2. For each claimed job, takes a transaction-level advisory lock on
   `(hashtext('pgseries.job'), job_id)` — a second concurrent driver
   that tries the same job is shed.
3. Runs the job body in a subtransaction; on error, records the error
   in `pgseries_information.job_errors` and reschedules with
   exponential backoff.
4. Releases the row lock by updating `running = false`,
   `next_run_at = …`.

Drivers (system cron, systemd timer, an app worker, an external
scheduler) call `run_due_jobs()` at any cadence ≥ 1 s; the function
is safe under N parallel drivers.

### 12. Migration

- `pgseries.adopt_partitioned_table(regclass)` — adopts an existing
  range-partitioned table without data movement.
- **From TimescaleDB:** preferred path is per-chunk parallel `pg_dump`.
  TimescaleDB chunks are individually `pg_dump`-able, so a parallel
  dump-and-restore over chunks is far faster than a single `COPY` on
  a multi-TB hypertable. The migration guide ships a script
  (`scripts/migrate-from-timescale.sh`) that enumerates source chunks,
  dumps in parallel, and restores into a fresh series table.
- Fallback path: `COPY` from TimescaleDB hypertable + bulk insert.
- No automated `hypertable → series_table` converter in v0.1.

### 13. Roles

`pgseries_reader`, `pgseries_writer`, `pgseries_admin`. All
`SECURITY DEFINER` functions pin
`SET search_path = pgseries, pg_catalog`. Functions that create chunks,
compress, decompress, or merge staging are owned by `pgseries_admin`
so chunk ownership is uniform across the series table; writers never
own a chunk (§1).

## Cardinality

v0.1 partitions on time only. Series cardinality is **not** the same
as the partitioning dimension — series identity lives in the segmentby
columns of the compressed sibling, not in the chunking scheme.

The practical ceiling depends on two distinct numbers:

- **Total active series** (`device_id` × `metric_name` × …): with
  segmentby compression, **10⁷–10⁸** active series is workable on a
  single node at typical metric rates.
- **Distinct segmentby values per chunk**: keep below **~10⁴** for the
  3–6× compression target. Above that, segments hold too few rows and
  ratio drops to ~1.5×; a workload-specific `add_compression_policy()`
  knob warns at 50 000.

The earlier "~10⁶ active series" framing in this spec was wrong — it
conflated total cardinality with per-chunk segmentby cardinality. Time-
only partitioning is sufficient at v0.1's target scale; `add_dimension`
(space partitioning) was deprioritized upstream and is not needed here.

## Non-goals

- Match TimescaleDB's 10–20× compression ratios. Target 3–6×.
- Match TimescaleDB scan latency on compressed data. Target: queries
  filter through BRIN/partition pruning before unnesting.

## Acceptance criteria

- `\i pgseries.sql` installs cleanly on stock PG 15–18 with
  `track_commit_timestamp = on`. Installer errors loudly when the
  GUC is off.
- **Plan-shape regression test** (§6): pgTAP test asserts partition
  pruning + BRIN `minmax_multi_ops` segment skipping + zero
  `Function Scan` on `unpack_segment` for skipped partitions. Runs in
  CI on PG 15/16/17/18.
- **Smoke test** on RDS, Aurora, Cloud SQL, Supabase, Neon (free tier;
  10M rows ingested over ≤10 minutes; SLA: each policy job completes
  within `2 × schedule_interval` measured at the 95th percentile over
  10 runs): create series table, ingest 10M rows, add retention + cagg
  + compression + recompression policies, verify each runs under
  `pg_cron` AND under `pgseries.run_due_jobs()`.
- **Compression benchmark** on a public dataset (NYC taxi + devops
  metrics): ratio (heap + TOAST, excl. indexes) and scan latency vs
  uncompressed and vs TimescaleDB on self-hosted PG, broken down by
  column type.
- **Continuous aggregate correctness:** out-of-order writes within
  `refresh_lag` are reconciled by the next refresh; writes outside
  the window appear in `pgseries_information.cagg_lag`. Straddle-bucket
  test asserts no double-count and no missed rows across the watermark.
- **Compressed write path:**
  - `error` mode: insert-blocker errors with the documented message.
  - `staging` mode: write → staging → recompression merge round-trips
    the same row count and aggregate values.
  - `auto_decompress` mode: round-trip correctness under concurrent
    writers (per-segment advisory lock test).
- **Read-only enforcement:** trigger raises on `INSERT`/`UPDATE`/
  `DELETE`/`TRUNCATE` against a compressed chunk; `revoke` denies
  before the trigger fires.
- **Compression swap:** `lock_timeout` retry test under contention;
  no orphaned partitions, no half-detached state.
- **Chunk lease vs retention:** retention against an actively
  compressing chunk waits or skips, never deadlocks.
- Red/green TDD for `pgseries-api`; `pgseries-core` covered by
  regression tests against PG 15/16/17/18.

## Open questions

- Default chunk size: time-based (1 day), row-based (1M), or adaptive?
- Segment row count: trade scan amplification against TOAST
  externalization. Default 1000; revisit after benchmarks.
- Lossy float requantization: opt-in per column at table creation, or
  per compression policy?
- Table abstraction name: `series_table` (default) vs `hypertable`
  (trademark check) vs other.
- `pgseries_information` view names: mirror `timescaledb_information`
  exactly (familiarity) or differentiate (avoid confusion when both
  systems coexist).
- Cagg select-list shapes: which of `HAVING` / `ROLLUP` / window
  functions can be lifted in v0.2 with a specified recombination rule?
- Commit-timestamp safety margin (§3): is 1 s enough on the slowest
  managed-PG provider observed in smoke tests?
