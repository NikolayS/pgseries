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
  `add_retention_policy()`, `add_compression_policy()`, `alter_job()`,
  `pause_job()`, `resume_job()`.

PostgreSQL 15+ (PG 14 lacks `lz4` as default TOAST compression and
`MERGE`; PG 17's `MERGE/SPLIT PARTITION` used opportunistically).

## Privileges

PgSeries needs no `shared_preload_libraries` of its own, but the
recommended scheduler is `pg_cron` (which does); a fallback exposes
every job as a SQL function callable from any external scheduler.

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
  columnar sibling.
- **Columnar compression** with explicit **segmentby** (scalar columns
  for tag filtering) and **orderby** (parallel arrays for time/value
  columns), frame-of-reference for ints/timestamps, per-column null
  bitmaps, dictionary encoding for low-cardinality text, TOAST/lz4 as
  the final stage. Target 3–6x.
- **Decompression** — explicit API for write-path access to compressed
  ranges.
- **Continuous aggregates** — incrementally-maintained rollup tables
  with an always-on invalidation log; refresh policy defines the
  reconciliation window (`refresh_lag` / `start_offset` / `end_offset`).
  Watermark anchored on `pg_snapshot_xmin()`, not wall-clock.
- **Real-time aggregation** — cagg view unions the materialized rollup
  with on-the-fly aggregation of source rows **past the materialization
  watermark** (the actual TimescaleDB behavior, not "uncompressed tail").
- **Finalized vs partials cagg storage** — finalized by default;
  `partials` opt-in for hierarchical caggs with non-aligned buckets.
- **Hierarchical caggs (cagg-on-cagg)** — when child bucket width is
  an integer multiple of parent's, aligned to a common epoch.
- **Reorder / clustering policies** — `CLUSTER` on chunks via cron.
- **`time_bucket()`** — pure SQL, full parity.
- **`time_bucket_gapfill()`** — pure SQL, single-group-key in v0.1.
- **`first()` / `last()` aggregates** — composite-state custom aggregates;
  `DISTINCT ON` recommended for hot paths.
- **Chunk exclusion / data skipping** — partition pruning on the
  compressed sibling plus BRIN over sidecar `seg_min_ts` / `seg_max_ts`
  / segmentby columns; the view rewrites time predicates to stats-column
  predicates so out-of-range segments never unnest.
- **Job scheduler** — `pg_cron`, plus a `pg_cron`-free fallback.
- **Job control API** — `alter_job`, `pause_job`, `resume_job`,
  `run_job_now`, retry-on-failure with exponential backoff.
- **Information views** — `pgseries_information.jobs`, `job_stats`,
  `job_errors`, `chunks`, `compression_settings`,
  `continuous_aggregates`, `compressed_chunk_stats`.
- **`ts_insert_blocker` analog** — write-path enforcement on
  compressed chunks (error by default; opt-in auto-decompress).
- **`show_chunks` / `drop_chunks`** — operational APIs.
- **Migration** — `pgseries.adopt_partitioned_table(regclass)` adopts
  an existing range-partitioned table in place; documented `COPY`-based
  path from TimescaleDB hypertables.
- **Roles** — `pgseries_reader` / `pgseries_writer` / `pgseries_admin`.

### Partial or lossy

- **Hyperfunctions** (`percentile_agg`, `histogram`, `counter_agg`,
  `time_weight`, ASOF, `locf`, `interpolate`) — slow in PL/pgSQL; lean
  on PG built-ins (`percentile_cont`, `width_bucket`) and `DISTINCT ON`.
- **Approximation sketches** — `hll` supported (broadly available on
  managed PG); `tdigest` dropped from the managed-PG happy path
  (unavailable on RDS/Aurora); pure-SQL t-digest is v0.2-conditional.
- **Space partitioning (`add_dimension`)** — deferred to v0.2; v0.1
  ceiling ~10⁶ active series.
- **Multi-group `time_bucket_gapfill`** — v0.1 single-group-key only.
- **Lossless float compression** — TimescaleDB uses Gorilla; v0.1 ships
  only opt-in lossy requantization. Lossless float compression awaits
  a viable pure-SQL codec.

### Out of scope (infeasible without C)

- **Bit-level codecs** — Gorilla, delta-delta, simple-8b. PL/pgSQL
  per-value overhead inverts the economics.
- **Custom planner and executor nodes** — vectorized compressed scan,
  chunk-exclusion planner hooks, constraint-aware parallel append.
  Replaced (imperfectly) by partition pruning + BRIN + view-layer
  predicate translation.
- **Distributed hypertables** — deprecated upstream as of TimescaleDB 2.14.
- **Tiered storage to object storage** — Timescale Cloud-only, requires
  custom storage hooks.
- **Promscale-style ingest caggs** — requires the bypassed insert path.

## In scope (v0.1)

### 1. Series tables
On native PG range partitioning by time. Chunks are pre-created by a
cron job ahead of the write frontier; a `BEFORE INSERT` trigger acts
only as a safety net (`pg_advisory_xact_lock` on the parent OID), so
the hot path never DDLs. Catalog tables: `pgseries.series_table`,
`pgseries.chunk`, `pgseries.dimension`.

### 2. Retention policies
`pg_cron` job calls `pgseries.drop_chunks(table, before := now() - N)`.
Chunks under active compression are leased in `pgseries.chunk` and
cannot be dropped concurrently.

### 3. Continuous aggregates
Incrementally-maintained rollup tables. An
`AFTER INSERT/UPDATE/DELETE` row trigger on the source table appends
`(min_time, max_time)` ranges to `pgseries.cagg_invalidation`; refresh
jobs replay and clear the log within `refresh_lag` / `start_offset` /
`end_offset`, matching TimescaleDB.

The materialization watermark uses
`pg_snapshot_xmin(pg_current_snapshot())` — not wall-clock — so
in-flight transactions cannot be skipped over (the PgQ tick pattern).

The cagg view returns the materialized rollup unioned with on-the-fly
aggregation of source rows past the materialization watermark — the
actual TimescaleDB real-time aggregation. Caggs default to finalized
form; opt-in `partials` mode enables hierarchical caggs across
non-aligned bucket widths.

Hierarchical caggs supported when child bucket width is an integer
multiple of parent's, aligned to a common epoch.

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
  sorted within each segment.
- **value columns** — parallel typed arrays.

Pipeline (vectorized SQL):

- group by segmentby, sort by orderby
- frame-of-reference for ints/timestamps: `min(col)` + `int2[]`/`int4[]`
  offsets, fall back to `int8[]` when range overflows
- per-column null bitmap (`bytea`)
- dictionary encoding for low-cardinality text (per-chunk `text[]` dict
  + `int2[]` indices)
- optional float requantization (lossy, opt-in per column; lossless FOR
  is integer/timestamp only)
- segment row count tuned so packed arrays land in TOAST EXTERNAL
  storage (above ~2 KiB) where lz4 actually runs; per-column
  `STORAGE EXTERNAL` set explicitly
- `STRICT READ ONLY` enforced via CHECK constraint during compression;
  swap uses `DETACH PARTITION CONCURRENTLY` then re-attach

Target ratio: **3–6x** against heap + TOAST (excluding indexes) on
integer/timestamp telemetry; lower on floats without bit-packed codecs.
Benchmarks report per-column-type breakdown.

### 5. Write path on compressed chunks

- **Default:** insert-blocker trigger errors with a message pointing at
  the decompression API.
- **Opt-in:** auto-decompress affected segments back into the
  uncompressed sibling, apply the write, mark for recompression by the
  next cron run.
- ON CONFLICT on compressed chunks always errors — no per-row primary
  key in compressed form.

### 6. Transparent compressed reads
A read view unions the uncompressed chunks with the unnested compressed
table, rewriting `time BETWEEN $a AND $b` into
`seg_max_ts >= $a AND seg_min_ts <= $b` so out-of-range segments skip
before unnesting. Plan shape is a verified acceptance criterion.

### 7. Data skipping without planner hooks
Sidecar stats columns on every compressed row: `seg_min_ts`,
`seg_max_ts`, `seg_row_count`, per-column null counts, segmentby
scalars. BRIN index over `(seg_min_ts, seg_max_ts)` with
`autosummarize=on` and tuned `pages_per_range`. Compressed tables are
append-only after creation; re-`CLUSTER` invalidates BRIN correlation
(documented).

### 8. Time helpers
`time_bucket()` and `time_bucket_gapfill()` in pure SQL
(`generate_series` + left join). Gapfill is single-group-key in v0.1;
multi-group + `locf`/`interpolate` deferred.

### 9. Reorder / clustering policies
`CLUSTER` on chunks via cron, only on chunks not under active
compression.

### 10. Aggregates
`first(value, time)` and `last(value, time)` as pure-SQL custom
aggregates with composite `(value, time)` state. `PARALLEL SAFE` only
after combinefunc verification. Convenience, not performance — use
`DISTINCT ON` on hot paths.

### 11. Operational surface
`pgseries_information` schema with views matching TimescaleDB naming
where reasonable: `jobs`, `job_stats`, `job_errors`, `chunks`,
`compression_settings`, `continuous_aggregates`,
`compressed_chunk_stats`. Job control: `alter_job`, `pause_job`,
`resume_job`, `run_job_now`, retry-on-failure with exponential backoff.

### 12. Migration

- `pgseries.adopt_partitioned_table(regclass)` — adopts an existing
  range-partitioned table without data movement.
- `COPY` from TimescaleDB hypertable + bulk insert into a fresh series
  table. No automated `hypertable → series_table` converter in v0.1.

### 13. Roles
`pgseries_reader`, `pgseries_writer`, `pgseries_admin`. All
`SECURITY DEFINER` functions pin
`SET search_path = pgseries, pg_catalog`.

## Cardinality ceiling

v0.1 partitions on time only, so every chunk holds every series.
Documented ceiling ~10⁶ active series; above that, `add_dimension`
(v0.2) is required.

## Non-goals

- Match TimescaleDB's 10–20x compression ratios. Target 3–6x.
- Match TimescaleDB scan latency on compressed data. Target: queries
  filter through BRIN/partition pruning before unnesting.

## Acceptance criteria

- `\i pgseries.sql` installs cleanly on stock PG 15–18.
- Plan-shape verification: `EXPLAIN (ANALYZE, BUFFERS)` on
  representative range queries shows partition pruning on the
  compressed sibling AND BRIN-driven segment skipping AND zero unnest
  of skipped segments — checked in CI on PG 15/16/17/18.
- Smoke test on RDS, Aurora, Cloud SQL, Supabase, Neon: create series
  table, ingest 10M rows, add retention + cagg + compression policies,
  verify each runs under `pg_cron` AND under the cron-free fallback.
- Compression benchmark on a public dataset (NYC taxi + devops
  metrics): ratio (heap + TOAST, excl. indexes) and scan latency vs
  uncompressed and vs TimescaleDB on self-hosted PG, broken down by
  column type.
- Continuous aggregate correctness: out-of-order writes within
  `refresh_lag` are reconciled by the next refresh; writes outside the
  window flagged in `pgseries_information.cagg_lag`.
- Compressed write path: insert-blocker errors with the documented
  message; opt-in decompression round-trips correctly.
- Red/green TDD for `pgseries-api`; `pgseries-core` covered by
  regression tests against PG 15/16/17/18.

## Open questions

- Default chunk size: time-based (1 day), row-based (1M), or adaptive?
- Segment row count: trade scan amplification against TOAST
  externalization.
- Lossy float requantization: opt-in per column at table creation, or
  per compression policy?
- Table abstraction name: `series_table` (default) vs `hypertable`
  (trademark check) vs other.
- `pgseries_information` view names: mirror `timescaledb_information`
  exactly (familiarity) or differentiate (avoid confusion when both
  systems coexist).
