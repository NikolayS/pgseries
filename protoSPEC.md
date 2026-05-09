# PgSeries — time-series for any Postgres

## What this is

A pure-SQL/PL-pgSQL time-series layer for Postgres 17+. Single-file
install (`\i pgseries.sql`). No C of our own. No
`shared_preload_libraries`. No restart. Apache-2.0.

It runs anywhere Postgres runs: any provider, any tier, any version
17 or newer. No vendor approval, no license carve-out, no parameter
group to edit, no support ticket. The install is one transaction; the
uninstall is one drop schema.

PgSeries is the time-series sibling of [PgQue](https://github.com/NikolayS/PgQue):
same install model, same Apache-2.0 license, same "anti-extension"
posture. PgQue's PgQ engine is **embedded** inside PgSeries — the
snapshot/tick semantics that PgQ has run in production since 2007
are exactly what continuous aggregates and the job scheduler need
(see *Architecture › Embedded PgQ*).

## Scale target

**v0.1 is sized for 10¹⁰ rows in a single series table** on a single
managed-PG instance. Every other number in this spec is derived
against that target.

Working back from 10B rows, with default 1-day chunks and 1-year
retention:

| | |
|---|---|
| Chunks | **365** (one per day) |
| Rows per chunk | ~**2.7 × 10⁷** |
| Average ingest | **~317 rows/sec** uniform |
| Raw heap+TOAST | ~**320 GiB** at 32 B/row |
| Compressed (3–6×) | **~55–105 GiB** |
| Per-chunk segmentby cap | **~10⁴** (compression-ratio constraint) |

10⁸–10⁹ rows is the easy case the same code handles without tuning.
Past 10¹⁰ on a single instance, two things break first: cagg refresh
on dense backfills (tunable), and PL/pgSQL compression throughput
per chunk (degrades gracefully to slower, never wrong).

**Reference machine for the scale benchmark**: single self-hosted
PG 18 instance with **local NVMe** storage and **~128 GiB RAM**.
Local NVMe is deliberate — the benchmark measures what the
architecture can do on real hardware, not what cloud-attached block
storage allows. Smoke tests on managed providers cover the
deployment path separately.

## Architecture

Each of the four big problems gets its own subsection here. Detailed
design lives in *Detailed design* further down; this section is the
map.

### 1. Time partitioning — pre-create ahead, default-partition catches misses

Series tables are native PG range-partitioned tables on the time
column. A cron job pre-creates partitions ahead of the write
frontier — typically a week ahead at default settings — so the hot
path is just PG's own row-routing, with no PgSeries code in it at
all.

For the rare write that arrives outside any pre-created range, PG
routes it to the table's `DEFAULT` partition. A `BEFORE INSERT`
trigger lives **only on the default partition**, so it fires only
when pre-creation has fallen behind. The trigger:

1. Claims a chunk-lease row (`for update skip locked` against
   `pgseries.chunk_lease`).
2. Creates the missing partition with the right bounds.
3. Re-routes the row by inserting into the parent, which now finds
   a matching partition.

Result: zero trigger overhead on the steady-state hot path. The
default partition exists but stays empty under healthy operation;
pre-creation lag is detectable as non-zero rows in `…_default`.

State coordination across pre-creation, compression, and retention
goes through `pgseries.chunk_lease` — a row per chunk with `state
in ('pending', 'ready', 'compressing', 'leased_for_write',
'compressed')`. Every operation that mutates chunk state takes
`for update skip locked` on the lease row. No advisory locks on
parent OIDs, no three-way deadlock between writers, retention, and
compression.

### 2. Continuous aggregates — embedded PgQ delivers the watermark

PgSeries has the same problem PgQ solves: when is it safe to
materialize a window of changes without missing in-flight
transactions, and without pinning the xmin horizon?

PgQ's answer is the **tick** — a row recording
`pg_current_snapshot()` at time T. Two consecutive ticks define a
batch: rows whose `xmin` is visible to tick N+1 but not tick N.
Apply that batch and you've made progress without missing anything.

PgSeries embeds the PgQ engine and reuses ticks as the cagg
materialization watermark, directly:

- An `AFTER STATEMENT` trigger on the source table emits one event
  per statement into a PgQ queue named `cagg_invalidation_<cagg>`.
  Each event carries `(min_time, max_time)` derived from the
  statement's transition tables. `AFTER ROW` triggers are
  deliberately not used — they serialize `COPY` and bulk insert.
- A refresh consumer takes the next batch (`pgque.next_batch`),
  reads its events, merges their time ranges (this is the
  invalidation-log coalescing — naturally part of batch
  consumption, not a separate maintenance job), and refreshes the
  rollup table for that range.
- The watermark *is* the batch boundary. Rows whose `xmin` is
  in-flight at batch-close don't appear in this batch; they appear
  in the next one. No xid-to-timestamp translation, no safety
  margin, no `track_commit_timestamp`. PgQ has been doing this
  since 2007.

The cagg view returns the materialized rollup `union all`'d with
on-the-fly aggregation of source rows past the watermark. v0.1
forbids `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`, and window
functions in cagg select lists — those shapes break the
union-then-regroup recombination. Wrap the cagg view in a regular
view to apply them at query time.

Hierarchical caggs (cagg-on-cagg) are supported only across
**fixed-width buckets** (microsecond … week) where the child
bucket is an integer multiple of the parent's. `month`/`year` are
not fixed-width and are forbidden in cagg parents in v0.1.

### 3. Compression — columnar siblings, BRIN-skipping, no planner hooks

Cold chunks are rolled into a parallel `_compressed` table that is
itself partitioned with the same time bounds. Each compressed row
holds N source rows, laid out columnar:

- **segmentby columns** (e.g. `device_id`) — scalar on the
  compressed row. Predicates on segmentby skip whole compressed
  rows without unnesting. This is the load-bearing decision; not
  the same as sort order.
- **orderby columns** (typically `time`) — parallel typed arrays,
  sorted within each segment. `orderby DESC` is supported per
  column at compression time so last-point queries can stop after
  one segment.
- **value columns** — parallel typed arrays.

The pipeline that builds a compressed row from source rows:

1. group by segmentby
2. sort by orderby
3. encode integers/timestamps with frame-of-reference (`min(col)` +
   `int2[]`/`int4[]`/`int8[]` offsets — falls back wider when the
   range overflows)
4. encode low-cardinality text with a per-chunk dictionary
   (`text[]` dict + `int2[]` indices)
5. encode nulls as a per-column bitmap (`bytea`)
6. optional lossy float requantization (opt-in per column)
7. write the column arrays to columns with `storage extended` so
   the values land out-of-line in TOAST and `lz4` runs on them
8. tune segment row count so the resulting arrays are above the
   TOAST threshold (~2 KiB) — below that, lz4 doesn't fire

Side stats columns on every compressed row (`seg_min_ts`,
`seg_max_ts`, `seg_row_count`, per-column null counts, segmentby
scalars) plus a BRIN `minmax_multi_ops` index on `(seg_min_ts,
seg_max_ts)` and on segmentby columns give us data skipping
without a custom planner. `minmax_multi_ops` (PG 14+) is required
because compressed rows arrive grouped by segmentby, not by time —
ranges from plain `minmax_ops` would be useless.

Reads go through a view that unions uncompressed + staging +
compressed siblings. Predicate pushdown is achieved with `LATERAL`
after a filtered subquery, never by `unnest` in the outer select —
the planner can't push quals through `unnest`. Plan shape is
verified in CI: a pgTAP test asserts partition pruning + BRIN
index scan + zero `Function Scan` on `unpack_segment` for skipped
partitions.

**Honest ratio.** Target **3–6×** against heap + TOAST on
integer/timestamp telemetry. Bit-level codecs (Gorilla,
delta-delta, simple-8b) would give higher ratios but their
PL/pgSQL per-value overhead inverts the economics. Float columns
without lossy requantization compress closer to 2×. Benchmarks
report per-column-type breakdown.

**Compression swap** takes a short `AccessExclusive` lock on the
parent under `set local lock_timeout = '1s'`, performs the heap
detach + compressed-sibling attach in one transaction, and
commits. `DETACH PARTITION CONCURRENTLY` is not used — its
two-phase semantics expose a window where readers see neither the
old nor the new partition for the same range. After swap, the
compression job calls `brin_summarize_new_values()` synchronously
so first-read skipping is immediate (autosummarize is async).

**Read-only enforcement** on compressed chunks is a `BEFORE INSERT
OR UPDATE OR DELETE OR TRUNCATE` trigger that raises, paired with
explicit `revoke insert, update, delete, truncate from
pgseries_writer` on every compressed chunk. `CHECK` constraints
constrain row values, not writes, and cannot enforce read-onlyness.

### 4. Late writes — staging sibling, no full decompress

Writes that arrive against an already-compressed chunk land in a
heap `_staging` sibling that shares the chunk's time range.
`add_recompression_policy()` is a separate cron job that
periodically merges staging rows into the compressed sibling.
Reads union all three siblings (uncompressed, staging, compressed)
so a freshly-written row is visible immediately.

This avoids decompressing a whole segment on every late write.
Two alternatives are also available:

- `error` mode: a trigger raises; the application is expected to
  not write to compressed ranges.
- `auto_decompress` mode: opt-in. Decompresses affected segments,
  applies the write, marks for recompression. A per-`(chunk_id,
  segment_hash)` advisory lock prevents two writers from racing
  on the same segment.

`on conflict` errors in all three modes — there is no per-row
primary key in compressed form. (PG general rule: `on conflict`
on a partitioned table requires the unique index to include the
partition key column.)

### 5. Embedded PgQ — three uses, one engine

PgQue's pgque-core is **vendored** into the PgSeries install bundle
as `pgseries/_pgq/`. The single-file install
(`\i pgseries.sql`) loads PgQ first, then PgSeries on top. The
schema is `pgseries_pgq` so it doesn't collide with a user's
existing PgQue install in the same database. PgQ is pinned to a
specific tagged release; PgSeries ships its own bundle.

Three places use it:

1. **Cagg invalidation queue** — described in §2 above. The
   batch boundary is the materialization watermark; nothing
   else needed.
2. **Job runner** — `pgseries.system_jobs` is a PgQ queue.
   Every scheduled policy (retention, compression,
   recompression, refresh, reorder, chunk pre-creation) emits
   a "due" event on its schedule and a consumer worker
   processes the next batch. The cron-free fallback story
   becomes trivial: anything that calls `pgque.ticker()` —
   `pg_cron`, `pg_timetable`, system cron, an app worker —
   drives PgSeries. Pause/resume is a PgQ-level operation.
3. **Staging → recompression signaling** — every write to a
   `_staging` sibling emits a "dirty segment" event. The
   recompression consumer batches by chunk and merges. Natural
   fit for batch boundaries.

Why vendor instead of depend: PgSeries has to give a single-file,
single-transaction install on any provider. Asking users to
install pgque first turns one transaction into two and one
namespace into a coordination problem. Vendoring is what PgQ does
internally with its three-table layout already; we're applying the
same trick one layer up.

### 6. Space partitioning — designed in, API in v0.2

Time-only partitioning is sufficient for the v0.1 10¹⁰-row scale
target. But every real time-series schema we've seen has a
non-temporal axis — `tenant_id`, `region`, `customer_id` — that
needs to participate in pruning and parallel write distribution.
Punting space partitioning to v0.2 *as an afterthought* would
require a catalog migration we don't want to do.

So: the catalog supports it from day zero. `pgseries.dimension`
holds one row per dimension on every series table, with
`dimension_type in ('time', 'space_hash', 'space_range')` and
`number_partitions` for space dimensions. v0.1 only writes
`dimension_type = 'time'` rows; the API surface
(`pgseries.add_dimension(table, column, type, n)`) exists and
returns a clear "v0.2" error.

`pgseries.chunk` carries dimension-coordinate columns
(`time_lower`, `time_upper`, `space_partition_index`) so a v0.2
chunk identity is a tuple, not a single time range. v0.1 chunks
all have `space_partition_index = 0`. Code paths that select
chunks (compression policy, retention, cagg refresh,
`drop_chunks`, `show_chunks`) accept a chunk *set*, not a chunk
range, so v0.2 can extend the selector without rewriting them.

What stays out: the v0.1 partition-creation code does not generate
sub-partitions. Adding it later means one new step in §1's
pre-creator job, not a redesign.

### 7. Job scheduling

There is no PgSeries-specific scheduler. PgQ's ticker model from
§5 is the scheduler. Three deployment shapes work out of the box:

- `pg_cron` calling `pgque.ticker_loop()` (PgQue's default).
- `pg_timetable` calling the same.
- An external driver (system cron, systemd timer, an app worker)
  calling `select pgque.ticker()` at any cadence ≥ 1 s.

Job bodies (compress chunk, refresh cagg, drop chunks, …) are
executed by a consumer of `pgseries.system_jobs`. Retry,
exponential backoff, and DLQ semantics are PgQ's, not ours.
`pgseries.alter_job` / `pause_job` / `resume_job` /
`run_job_now` are thin wrappers over PgQ's queue operations.

## Detailed design

### Series tables

Native PG range partitioning by time. A `DEFAULT` partition exists
on every series table. The pre-creator runs as a PgQ job
(`pgseries.system_jobs` event type `precreate_chunks`) and creates
N partitions ahead of the write frontier (default `pre_create_n =
7` for daily chunks). The hot path involves no PgSeries code.

The default partition's `BEFORE INSERT` trigger is the rare-miss
fallback. It claims a `chunk_lease` row, creates the missing
partition with `attach partition`, and re-inserts the row through
the parent. If the row racing in is one of many that would all
target the same missing range, only the first claims the lease;
the rest find `state = 'ready'` after re-insert and route directly.
Throughput collapses to write-serial only for the first row of a
new range and only when pre-creation has lagged.

`SECURITY DEFINER` ownership reconciliation: pre-creator and
default-partition trigger functions are owned by `pgseries_admin`,
so created chunks inherit `pgseries_admin` ownership uniformly.
Writers get table-level grants via `pgseries_writer` membership on
the parent. No writer ever owns a chunk.

Catalog: `pgseries.series_table`, `pgseries.chunk`,
`pgseries.chunk_lease`, `pgseries.dimension`.

### Retention

A scheduled `drop_chunks` job consumes from `pgseries.system_jobs`
and drops chunks with `time_upper < now() - retention_period`.
Chunks with `state in ('compressing', 'leased_for_write')` in
`pgseries.chunk_lease` are skipped this round and retried on the
next.

### Continuous aggregates

`AFTER STATEMENT` trigger using transition tables produces one
PgQ event per statement on `cagg_invalidation_<cagg>` carrying
`(min_time, max_time)`. Refresh consumer takes the next batch,
unions ranges, and refreshes the materialized rollup for the
unioned span. The cagg view's "real-time" half reads source rows
with `time >= materialization_watermark` (the time floor of the
next pending batch) and unions with the materialized half.
Straddle buckets are correct by construction: the materialized
half never closes a partial bucket; the real-time half handles
everything past the watermark.

Hierarchical caggs: child width must be an integer multiple of
parent's width, both fixed-width, both aligned on a common epoch.
`month`/`year` rejected by `add_continuous_aggregate()` when used
as a parent of a cagg-on-cagg.

### Compression — pipeline details

Per-column rules in pipeline order:

| Stage | Operation |
|---|---|
| group | `group by` segmentby columns |
| sort | `order by` orderby columns within each segment |
| FOR (int) | `min` + `int2[]`/`int4[]`/`int8[]` offsets, widest needed |
| FOR (timestamp) | same, on `epoch_microseconds` |
| dict (text) | `text[]` dictionary per chunk, `int2[]` indices on rows |
| nulls | `bytea` bitmap, one bit per source row per nullable column |
| float (lossy, opt-in) | quantize to N significant digits before FOR |
| storage | `alter column … set storage extended` (default; lz4 runs) |
| segment row count | tuned so packed arrays are > TOAST threshold (~2 KiB) |

`storage extended` is the correct class. An earlier version of
this spec said `storage external`, which is out-of-line
*uncompressed* — `lz4` would never fire. `extended` is what we
want.

`add_compression_policy()` warns when a chunk's distinct
segmentby count exceeds `segmentby_warn_threshold` (default
50 000); the practical cap for the 3–6× target is ~10⁴. Above
that, segments hold too few rows, FOR overhead dominates, and
ratio drops to ~1.5×.

### BRIN data skipping

`minmax_multi_ops` indexes on `(seg_min_ts, seg_max_ts)` and on
each segmentby column. `pages_per_range` defaults to 32;
`add_compression_policy()` accepts an override.
`brin_summarize_new_values()` is called synchronously after the
compression swap so the first range query skips correctly without
waiting for autovacuum.

### Read view

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
      -- segmentby predicates pushed here too
  ) f,
  lateral pgseries.unpack_segment(f.compressed_row) as c(unpacked);
```

The filtered subquery is the load-bearing shape: it lets quals
land on the BRIN/partition scan before the lateral unnest fires.
A `select unnest(...)` in the outer select can't push filters
through.

CI runs a pgTAP plan-shape test on a representative range query
and asserts partition pruning, BRIN index choice referencing
`seg_min_ts`/`seg_max_ts`, no `Function Scan` on
`unpack_segment` for skipped partitions, and segmentby predicates
in the inner filter.

### Compression swap

```sql
set local lock_timeout = '1s';
alter table <table> detach partition <chunk> finalize;
alter table <table> attach partition <chunk_compressed>
  for values from (...) to (...);
```

If `lock_timeout` fires, the job retries on the next tick with
truncated exponential backoff. No state is left half-applied.
Plan-cache invalidation is unavoidable and is documented.

### Write path on compressed chunks

Three modes, picked per series table at `add_compression_policy`
time: `error` (default), `staging` (recommended for late writes),
`auto_decompress` (opt-in). `staging` and `auto_decompress` both
take per-`(chunk_id, segment_hash)` advisory locks so two writers
cannot race to decompress and recompress the same segment.

### Time helpers

`time_bucket()` and `time_bucket_gapfill()` in pure SQL, fixed-
width buckets (microsecond … week). `month`/`year` accepted by
`time_bucket()` itself; not accepted as cagg parents (see §3 of
*Architecture*). Gapfill is single-group-key in v0.1; multi-group
+ `locf`/`interpolate` deferred.

### Aggregates

`first(value, time)` and `last(value, time)` as pure-SQL custom
aggregates with composite `(value, time)` state. `parallel safe`
only after combinefunc verification. Convenience, not performance
— use `distinct on` on hot paths. Last-point queries on
compressed data benefit from `orderby desc` (above) so unpack
stops after one segment.

### Operational surface

`pgseries_information` schema:

- `jobs`, `job_stats`, `job_errors` — system_jobs queue state
- `chunks`, `compression_settings`, `compressed_chunk_stats`
- `continuous_aggregates`, `cagg_lag` — per-cagg
  `(materialization_watermark, latest_invalidation_time,
  lag_seconds, out_of_window_writes)`

Job control: `alter_job`, `pause_job`, `resume_job`, `run_job_now`.

### Migration

- `pgseries.adopt_partitioned_table(regclass)` — adopt an existing
  range-partitioned table without data movement.
- From a hypertable: per-chunk parallel `pg_dump` is the preferred
  path. Hypertable chunks are individually `pg_dump`-able, so a
  parallel dump-and-restore is far faster than a single `COPY`.
  `scripts/migrate-from-hypertable.sh` enumerates source chunks,
  dumps in parallel, and restores into a fresh series table.
- Fallback: `COPY` into a fresh series table.

### Roles

`pgseries_reader`, `pgseries_writer`, `pgseries_admin`. All
`SECURITY DEFINER` functions pin
`set search_path = pgseries, pg_catalog`. Functions that create
chunks, compress, decompress, or merge staging are owned by
`pgseries_admin` so chunk ownership is uniform.

## Feature mapping (for TimescaleDB users)

A reference for readers familiar with TimescaleDB's surface, so
you can tell at a glance what maps to what. This is not a parity
goal — the real commitments are in *Architecture* and
*Non-goals*.

### Supported in v0.1

| TimescaleDB | PgSeries |
|---|---|
| Hypertables | Series tables (native PG range partitioning + default-partition catch) |
| Chunks | Same — one PG partition per chunk |
| Retention policies | `add_retention_policy` (PgQ-scheduled) |
| Compression policies | `add_compression_policy` |
| Recompression | `add_recompression_policy` (separate scheduled policy) |
| Columnar compression | Sibling `_compressed` table; segmentby + orderby; FOR + dict + null bitmap + lz4. Target 3–6× |
| Decompression API | `decompress_chunk` |
| Continuous aggregates | `add_continuous_aggregate` over an embedded PgQ invalidation queue |
| Real-time aggregation | Cagg view unions materialized + on-the-fly past the PgQ tick |
| Finalized vs partials cagg storage | Both; finalized default, `partials` opt-in for hierarchical |
| Hierarchical caggs (cagg-on-cagg) | Fixed-width buckets only (microsecond … week) |
| Reorder / clustering policies | `CLUSTER` on chunks via PgQ schedule |
| `time_bucket()` | Pure SQL, fixed-width parity |
| `time_bucket_gapfill()` | Pure SQL, single-group-key in v0.1 |
| `first()` / `last()` | Pure-SQL custom aggregates with composite state |
| Chunk exclusion / data skipping | Partition pruning + BRIN `minmax_multi_ops` + view-layer predicate pushdown via `LATERAL` |
| Job scheduler | PgQ ticker (`pg_cron`, `pg_timetable`, or any external driver) |
| Job control | `alter_job`, `pause_job`, `resume_job`, `run_job_now`, retry-with-backoff, DLQ |
| `timescaledb_information` views | `pgseries_information.{jobs, job_stats, job_errors, chunks, compression_settings, compressed_chunk_stats, continuous_aggregates, cagg_lag}` |
| `ts_insert_blocker` | Default-mode trigger on compressed chunks (raises); also `staging` and `auto_decompress` modes |
| `show_chunks` / `drop_chunks` | Same names |
| Roles | `pgseries_reader`, `pgseries_writer`, `pgseries_admin` |

### Partial or lossy

| TimescaleDB | PgSeries |
|---|---|
| Hyperfunctions (`percentile_agg`, `histogram`, `counter_agg`, `time_weight`, ASOF, `locf`, `interpolate`) | Use PG built-ins (`percentile_cont`, `width_bucket`) and `DISTINCT ON` instead. Hand-rolled PL/pgSQL versions are too slow to ship. |
| Approximation sketches | `hll` supported (broadly available on managed PG); `tdigest` not on the managed-PG happy path; pure-SQL t-digest is v0.2-conditional. |
| Multi-group `time_bucket_gapfill` | Single-group-key only in v0.1. |
| Lossless float compression | Gorilla unavailable in PL/pgSQL; v0.1 ships only opt-in lossy requantization. Floats compress closer to 2× without it. |
| Cagg select-list shapes | `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`, and window functions rejected by `add_continuous_aggregate()`. Wrap the cagg view in a regular view to apply them at query time. |
| Compression ratio | Target **3–6×** (vs Timescale's 10–20× with bit-level codecs). |

### Out of scope (v0.1 or never)

| TimescaleDB | PgSeries |
|---|---|
| Bit-level codecs (Gorilla, delta-delta, simple-8b) | Never in pure SQL. Per-value PL/pgSQL overhead inverts the economics. |
| Custom planner/executor nodes (vectorized scan, chunk-exclusion hooks, constraint-aware parallel append) | Never without C. Replaced (imperfectly) by partition pruning + BRIN + `LATERAL`. |
| Distributed hypertables | Out of scope. Deprecated upstream as of TimescaleDB 2.14. |
| Tiered storage to object storage | Out of scope. Requires custom storage hooks. |
| Promscale-style ingest caggs | Out of scope. Requires the bypassed insert path. |
| `add_dimension` (space partitioning) | **v0.2** — designed into the catalog from day zero (see *Architecture* §6). API exists in v0.1 and returns a v0.2 error. |
| Hypertable → series_table converter | v0.1 ships a per-chunk parallel `pg_dump` migration script, not an in-place converter. |

## Non-goals

- Match TimescaleDB-class **10–20×** compression ratios. Target
  3–6×. Bit-level codecs are not feasible in PL/pgSQL.
- Match TimescaleDB scan latency on compressed data. Target:
  queries filter through BRIN + partition pruning before
  unnesting; that's enough for analytics, not for sub-millisecond
  point lookups.
- Scale a single series table past 10¹⁰ rows without
  `add_dimension` (v0.2).
- Support PG 16 or older. Greenfield projects should be on a
  modern PG; PgSeries leans on PG 17 features (`MERGE`,
  `MERGE/SPLIT PARTITION`, modern partitioning ergonomics) for
  simpler code.

## Acceptance criteria

- **Install.** `\i pgseries.sql` installs cleanly on stock PG 17
  and PG 18 in a single transaction. The bundle includes pgque-core
  loaded into a `pgseries_pgq` schema.
- **Plan-shape regression test.** pgTAP test asserts partition
  pruning + BRIN `minmax_multi_ops` segment skipping + zero
  `Function Scan` on `unpack_segment` for skipped partitions.
  Runs in CI on PG 17 and PG 18.
- **Smoke test.** RDS, Aurora, Cloud SQL, Supabase, Neon, Crunchy
  Bridge (any tier with PG 17+ available; 10⁷ rows ingested over
  ≤10 minutes; SLA: each policy job completes within `2 ×
  schedule_interval` measured at the 95th percentile over 10
  runs). Verify each policy runs under `pg_cron` and under PgQue's
  ticker pattern from an external driver.
- **Scale benchmark.** A single self-hosted PG 18 instance
  ingests **10¹⁰ rows** into one series table over 30 days of
  synthetic metric workload, with retention + cagg + compression
  + recompression policies all running. **Reference hardware**:
  local NVMe (no network-attached block storage), ~128 GiB RAM,
  modern many-core x86. Reported numbers: ingest p50/p95, cagg
  refresh p95 latency, compression throughput (rows/sec/chunk),
  on-disk size (compressed vs heap+TOAST), p95 query latency on
  representative range scans. Exact CPU count and NVMe throughput
  floor pinned after the first dry-run.
- **Compression benchmark.** NYC taxi + devops-metrics public
  datasets: ratio (heap + TOAST, excl. indexes) and scan latency
  on PgSeries vs uncompressed PG, broken down by column type.
- **Cagg correctness.** Out-of-order writes within `refresh_lag`
  are reconciled by the next refresh; writes outside the window
  appear in `pgseries_information.cagg_lag`. Straddle-bucket test
  asserts no double-count and no missed rows across the watermark.
- **Compressed write path.**
  - `error` mode raises with the documented message.
  - `staging` mode round-trips row count and aggregate values
    through the recompression merge.
  - `auto_decompress` mode round-trips correctly under
    concurrent writers (per-segment advisory lock test).
- **Read-only enforcement.** Trigger raises on `INSERT`/
  `UPDATE`/`DELETE`/`TRUNCATE` against a compressed chunk;
  `revoke` denies before the trigger fires.
- **Compression swap.** `lock_timeout` retry test under contention;
  no orphaned partitions, no half-detached state.
- **Chunk lease vs retention.** Retention against an actively
  compressing chunk waits or skips, never deadlocks.
- **Default-partition catch.** Pre-creation deliberately disabled;
  inserts arrive against ranges with no partition; default-
  partition trigger creates the partition, re-routes the row, no
  rows left in `…_default` after a healthy tick.
- **Space-partition catalog forward-compatibility.** v0.1 catalog
  round-trips a `dimension_type = 'space_hash'` row written by a
  v0.2 simulation harness without schema migration.
- Red/green TDD for `pgseries-api`; `pgseries-core` covered by
  regression tests against PG 17 and PG 18.

## Open questions

- Default chunk size: time-based (1 day), row-based (~3 × 10⁷),
  or adaptive? At the 10¹⁰-row target the two converge on ~1 day.
- Segment row count default. 1 000 to start; revisit after
  benchmarks.
- Lossy float requantization: opt-in per column at table creation,
  or per compression policy?
- Table abstraction name: `series_table` (default) vs
  `hypertable` (trademark check) vs other.
- `pgseries_information` view names: mirror existing time-series
  conventions (familiarity) or differentiate?
- Cagg select-list shapes: which of `having` / `rollup` / window
  functions can be lifted in v0.2 with a specified recombination
  rule?
- Embedded-PgQ schema collision: is `pgseries_pgq` distinctive
  enough, or do we want a per-series-table prefix?
- v0.2 `add_dimension` API shape: hash partitions only, or hash +
  range? List partitioning is rare in time-series and probably
  out of scope.
