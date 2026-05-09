# PgSeries — Managed-PG-Compatible Time-Series Layer

## Problem

TimescaleDB delivers hypertables, continuous aggregates, retention, and
columnar compression — but ships as a C extension that requires
`shared_preload_libraries` and is unavailable on most managed Postgres
(RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon, Crunchy Bridge).
Users on those platforms have no native time-series story.

## Goal

Build a pure-SQL/PL-pgSQL time-series layer in the PgQue style: single-file
install (`\i pgseries.sql`), no C code of our own, no
`shared_preload_libraries` from PgSeries itself, no restart. Two-layer
design mirroring PgQue:

- **pgseries-core:** chunk lifecycle, retention, rollup engine,
  compression pipeline.
- **pgseries-api:** `create_series_table()`, `add_continuous_aggregate()`,
  `add_retention_policy()`, `add_compression_policy()`, `alter_job()`,
  `pause_job()`, `resume_job()`.

PostgreSQL 15+ (PG 14 lacks `lz4` as default TOAST compression and
`MERGE`; PG 17 adds `MERGE/SPLIT PARTITION` which we use opportunistically).

## Honest Privilege Statement

`pg_cron` itself requires `shared_preload_libraries` and a privileged role
to install (`rds_superuser` on RDS, equivalent on Aurora / Cloud SQL /
AlloyDB; pre-installed and toggle-able on Supabase and Neon). PgSeries
does not require `shared_preload_libraries` for its own code, but the
**recommended** install path uses `pg_cron` and inherits its privilege
requirements. PgSeries also ships a `pg_cron`-free fallback: every job
is exposed as a SQL function (`pgseries.run_compression()`,
`pgseries.run_retention()`, `pgseries.refresh_caggs()`) callable from
any external scheduler or `LISTEN/NOTIFY` worker.

## Naming

- `pgseries` — schema, package, repo, CLI, function prefix
- `PgSeries` — prose, headings, README

## In Scope (v0.1)

### 1. Series tables
On top of native PG declarative range partitioning by time. **Chunks are
pre-created** by a cron job ahead of the write frontier. A
`BEFORE INSERT` trigger acts only as a safety net (creates the missing
chunk under `pg_advisory_xact_lock` on the parent OID) — the hot path
never DDLs, so concurrent writers do not serialize on
`AccessExclusiveLock`. Per-table config in `pgseries.series_table`,
`pgseries.chunk`, and `pgseries.dimension` catalog tables.

### 2. Retention policies
`pg_cron` job calls `pgseries.drop_chunks(table, before := now() - N)`.
Policy ordering is enforced: a chunk under active compression cannot be
dropped (chunk maintenance lease in `pgseries.chunk` blocks retention).

### 3. Continuous aggregates
Incrementally-maintained rollup tables. **Always-on invalidation log:**
an `AFTER INSERT/UPDATE/DELETE` row trigger on the source table appends
`(min_time, max_time)` ranges to `pgseries.cagg_invalidation`. Refresh
jobs replay and clear the log within a configurable window
(`refresh_lag`, `start_offset`, `end_offset`), matching TimescaleDB
semantics.

The materialization watermark is anchored on
`pg_snapshot_xmin(pg_current_snapshot())` — not wall-clock — so
in-flight transactions cannot be skipped over (this is the PgQ tick
pattern, reused).

The cagg query view returns the materialized rollup unioned with
on-the-fly aggregation of source rows **past the materialization
watermark** (the TimescaleDB "real-time aggregation" behavior, correctly
defined). Caggs default to **finalized form** (storing final aggregate
values, not partials) for storage efficiency; opt-in `partials` mode
enables hierarchical caggs across non-aligned bucket widths.

Hierarchical caggs (cagg-on-cagg) supported when child bucket width is
an integer multiple of parent bucket width, aligned to a common epoch.

### 4. Columnar compression (pure SQL)
Cold chunks are rolled into a sibling `_compressed` table that is
**itself partitioned** with the same time bounds as the source — so
partition pruning fires on the compressed side before the read view ever
unnests. Each compressed row stores N source rows with explicit
**segmentby** and **orderby** semantics:

- **segmentby columns** (e.g. `device_id`, `metric_name`) — stored as
  scalar columns on the compressed row. Predicates on segmentby skip
  whole compressed rows without unnesting. This is the single most
  important correctness/performance feature; it must not be conflated
  with sort order.
- **orderby columns** (typically `time`) — stored as parallel typed
  arrays. Within a segment, rows are sorted by orderby before packing.
- **value columns** — stored as parallel typed arrays.

Compression pipeline, all vectorized SQL:

- group by segmentby, sort by orderby
- frame-of-reference for integer and timestamp columns: store
  `min(col)` + `int2[]`/`int4[]` offsets, fall back to `int8[]` when
  range exceeds the smaller types
- per-column null bitmap (`bytea`) for nullable columns
- dictionary encoding for low-cardinality text columns (per-chunk
  dictionary as `text[]`, values as `int2[]` indices)
- optional float requantization (lossy, opt-in per column at table
  creation; lossless FOR applies to integer/timestamp only)
- target segment row count tuned so packed arrays land in TOAST
  EXTERNAL storage (above ~2 KiB) where lz4 actually runs; per-column
  `STORAGE EXTERNAL` set explicitly
- `STRICT READ ONLY` enforced via CHECK constraint on chunks under
  compression; chunk swap uses `DETACH PARTITION CONCURRENTLY` then
  re-attach to avoid blocking readers

Compression ratio target: **3–6x** measured against on-disk uncompressed
chunk size including TOAST, excluding indexes, on integer/timestamp
telemetry. Ratios on float telemetry without bit-packed codecs typically
land lower; the benchmark must report per-column-type breakdown rather
than a single headline number.

### 5. Write path on compressed chunks
INSERT/UPDATE/DELETE/COPY into a compressed chunk's time range:

- **Default:** error via insert-blocker trigger with a clear message
  pointing at the decompression API.
- **Opt-in:** auto-decompress the affected segments back into the
  uncompressed sibling, apply the write, mark the chunk for
  recompression by the next cron run.
- ON CONFLICT on compressed chunks always errors — there is no
  per-row primary key in the compressed form.

### 6. Transparent compressed reads
The user-facing series table is exposed via a view that unions the
uncompressed chunks with the unnested compressed table. The view
explicitly translates time-range predicates into stats-column
predicates (`time BETWEEN $a AND $b` → `seg_max_ts >= $a AND
seg_min_ts <= $b`) so segments outside the query range skip before
unnesting. The plan shape is a verified acceptance criterion (see
below).

### 7. Data skipping without planner hooks
Sidecar stats columns on every compressed row: `seg_min_ts`,
`seg_max_ts`, `seg_row_count`, per-column null counts, segmentby
scalars. BRIN index over `(seg_min_ts, seg_max_ts)` with
`autosummarize=on` and tuned `pages_per_range`. Compressed tables are
append-only after creation; documented that re-`CLUSTER` invalidates
BRIN correlation.

### 8. Time helpers
`time_bucket()` and `time_bucket_gapfill()` in pure SQL
(`generate_series` + left join). Gapfill is scoped to single-group-key
queries in v0.1; multi-group gapfill and integration with `locf` /
`interpolate` deferred.

### 9. Reorder / clustering policies
`CLUSTER` on chunks via cron, only on chunks not under active
compression.

### 10. Aggregates
`first(value, time)` and `last(value, time)` as pure-SQL custom
aggregates with composite `(value, time)` state. Marked `PARALLEL
SAFE` only after combinefunc verification. Documented as "use
`DISTINCT ON` instead for hot paths" — these aggregates are convenience,
not performance.

### 11. Operational surface
A `pgseries_information` schema with views matching TimescaleDB's
naming where reasonable: `jobs`, `job_stats`, `job_errors`, `chunks`,
`compression_settings`, `continuous_aggregates`, `compressed_chunk_stats`.
Job control API: `alter_job`, `pause_job`, `resume_job`,
`run_job_now`, configurable retry-on-failure with exponential backoff.

### 12. Migration
- `pgseries.adopt_partitioned_table(regclass)` — converts an existing
  range-partitioned table to a series table without data movement.
- Documented TimescaleDB export path: `COPY` from hypertable + bulk
  insert into a fresh series table. No automated `hypertable →
  series_table` converter in v0.1.

### 13. Roles
`pgseries_reader`, `pgseries_writer`, `pgseries_admin`. All
`SECURITY DEFINER` functions pin `SET search_path = pgseries,
pg_catalog`.

## Cardinality Ceiling

v0.1 uses time-only partitioning. Workloads above ~10⁵ active series in
a single time window will see segmentby-only pruning carry the load,
since every chunk holds every series. **Documented v0.1 ceiling:
~10⁶ active series.** Space partitioning (`add_dimension` analog) is
v0.2.

## Partial / Lossy

- **Hyperfunctions** (`percentile_agg`, `histogram`, `counter_agg`,
  `time_weight`, ASOF/`locf`/`interpolate`) — implementable in
  PL/pgSQL but slow on hot paths. Lean on PG built-ins
  (`percentile_cont`, `width_bucket`).
- **Approximation sketches** — `hll` is broadly available on managed
  PG; `tdigest` is **not** available on RDS or Aurora and is dropped
  from the managed-PG happy path. v0.2 may ship a pure-SQL t-digest if
  performance is acceptable.

## Out of Scope

- Bit-level codecs (Gorilla, delta-delta, simple-8b) — PL/pgSQL
  interpreter overhead inverts the economics.
- Custom planner / executor nodes, vectorized scan — C-only.
- Distributed hypertables — deprecated upstream anyway.
- Tiered storage to object storage — Timescale Cloud-only feature.

## Non-Goals

- Match TimescaleDB's 10–20x compression ratios. Target 3–6x via the
  vectorized-SQL stack above.
- Match TimescaleDB scan latency on compressed data. Target is "good
  enough that BRIN-skipped queries dominate" — most analytical queries
  touch a small fraction of segments.

## Acceptance Criteria

- `\i pgseries.sql` installs cleanly on stock PG 15–18.
- Plan-shape verification: `EXPLAIN (ANALYZE, BUFFERS)` on representative
  range queries against the read view shows partition pruning on the
  compressed sibling AND BRIN-driven segment skipping AND zero
  unnest of skipped segments — checked in CI on PG 15/16/17/18.
- Smoke test on RDS, Aurora, Cloud SQL, Supabase, Neon: create series
  table, ingest 10M rows, add retention + cagg + compression policies,
  verify each runs under `pg_cron` AND under the cron-free fallback.
- Compression benchmark on a public dataset (NYC taxi + devops metrics):
  report ratio (heap + TOAST, excl. indexes) and scan latency vs
  uncompressed and vs TimescaleDB on self-hosted PG, broken down by
  column type.
- Continuous aggregate correctness test: out-of-order writes within
  `refresh_lag` window are reconciled by the next refresh; writes
  outside the window are flagged in `pgseries_information.cagg_lag`.
- Write-path on compressed chunks: insert-blocker errors with the
  documented message; opt-in decompression round-trips correctly.
- Red/green TDD for all `pgseries-api` code; `pgseries-core` covered
  by regression tests against PG 15/16/17/18.

## Open Questions

- Default chunk size: time-based (1 day), row-based (1M), or adaptive?
- Sidecar stats granularity: per compressed row (~1000 source rows)
  is the only design that pays for itself; the real choice is segment
  row count, which trades scan amplification against TOAST
  externalization.
- Lossy float requantization: opt-in per column at table creation, or
  per compression policy?
- Naming for the table abstraction: `series_table` (default) vs
  reusing `hypertable` (trademark check needed) vs something else.
- `pgseries_information` view naming: mirror `timescaledb_information`
  exactly (familiarity) or differentiate (avoid confusion when both
  systems coexist on one cluster).
