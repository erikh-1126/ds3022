# nyc_taxi_dbt

A minimal dbt project transforming NYC yellow taxi trip data, using
DuckDB as the adapter so it reads parquet directly over HTTPS — no
warehouse, download, or load step needed.

This project is being built up gradually across three classes. Models,
tests, and marts are released class by class, so your copy may contain
only part of what's described below.

## Layout

```
nyc_taxi_dbt/
├── dbt_project.yml
├── profiles.yml.example        # copy to ~/.dbt/profiles.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml               # points at the raw parquet (local or remote)
│   │   ├── stg_yellow_tripdata.sql   # cast/clean raw trip columns
│   │   ├── stg_taxi_zones.sql        # cast/clean the zone lookup (LocationID -> name)
│   │   └── schema.yml                # column tests on the staging models
│   └── marts/
│       ├── fct_trips.sql                 # derived duration/speed/tip metrics per trip
│       ├── mart_hourly_demand.sql        # demand by hour of day
│       ├── mart_monthly_demand.sql       # demand by month
│       ├── mart_daily_summary.sql        # daily volume, revenue, speed
│       ├── mart_speed_by_hour_dow.sql    # speed by day of week x hour
│       ├── mart_location_rankings.sql    # top/bottom 10 zones by pickups/dropoffs
│       ├── mart_dow_summary.sql          # volume/fare/tips by day of week
│       ├── mart_trip_distance_distribution.sql  # 1-mile distance histogram
│       ├── mart_tips_by_payment_type.sql # tipping by payment type
│       ├── mart_fare_efficiency.sql      # $/mile and $/minute by distance band
│       ├── mart_top_corridors.sql        # top 25 pickup -> dropoff zone pairs
│       ├── mart_passenger_count.sql      # trip traits by passenger count
│       ├── mart_vendor_comparison.sql    # per-vendor metrics
│       └── schema.yml
├── packages.yml                # dbt_utils, installed with `dbt deps`
├── plots/                      # scripts that chart the marts (output PNGs are git-ignored)
└── tests/
    ├── assert_no_zero_or_null_trips.sql       # business-rule test on fct_trips
    ├── assert_no_unrealistic_trip_speeds.sql  # business-rule test on fct_trips
    ├── assert_dropoff_after_pickup.sql        # business-rule test on staging
    └── assert_*_dynamics.sql                  # expected data behavior, one per mart (#2, #4, #6-#10)
```

| Mart | Answers (`QUESTIONS.md`) | Grain |
|---|---|---|
| `mart_hourly_demand` | #1 daily demand curve | one row per hour (0-23) |
| `mart_monthly_demand` | #3 seasonality | one row per month |
| `mart_daily_summary` | daily rollup for dashboards/reports | one row per day |
| `mart_speed_by_hour_dow` | #5 speed as a congestion proxy | one row per (day of week, hour) |
| `mart_location_rankings` | busiest/quietest zones | one row per (location type, top/bottom, rank) |
| `mart_dow_summary` | #2 weekday vs. weekend | one row per day of week |
| `mart_trip_distance_distribution` | #4 distance distribution / cutoffs | one row per 1-mile bin |
| `mart_tips_by_payment_type` | #6 tipping by payment type | one row per payment type |
| `mart_fare_efficiency` | #7 $/mile and $/minute | one row per distance band |
| `mart_top_corridors` | #8 popular pickup/dropoff pairs | one row per top-25 zone pair |
| `mart_passenger_count` | #9 passenger count patterns | one row per passenger count |
| `mart_vendor_comparison` | #10 vendor comparison | one row per vendor |

## Setup

1. Install: `pip install dbt-duckdb`
2. Copy `profiles.yml.example` to `~/.dbt/profiles.yml`.
3. From the `nyc_taxi_dbt/` directory:

```bash
dbt deps         # install packages (dbt_utils)
dbt debug        # confirm the connection
dbt run          # build the staging models, fct_trips, and the marts
dbt test         # run column tests + the singular tests in tests/
```

4. Query the result:

```bash
duckdb nyc_taxi.duckdb -c "select * from fct_trips limit 10;"
```

No local data file to download — `sources.yml` points `external_location`
at TLC's public HTTPS parquet host, and DuckDB's `httpfs`/`parquet`
extensions stream it on demand. That same `external_location` key can
just as easily point at a local file path or an `s3://` URL — see the
comments in `sources.yml`.

## Why `{{ }}`? (Jinja + `ref()`)

The `.sql` files in `models/` aren't plain SQL — they're SQL *templates*
written in [Jinja](https://jinja.palletsprojects.com/), the same
templating language Flask/Django use for HTML. dbt compiles each
template into real SQL before running it.

The construct you'll see everywhere is `{{ ref('model_name') }}`, e.g.
in `fct_trips.sql`:

```sql
with trips as (
    select * from {{ ref('stg_yellow_tripdata') }}
)
```

`ref()` is a function call, not a table name — dbt resolves it at
compile time to the actual table/view in whatever database and schema
the current target points at. Two things fall out of that:

- **No hardcoded table names.** The same model compiles correctly
  whether it's running against your local `dev` target or a
  teammate's, without editing SQL.
- **Automatic lineage.** Because every `ref()` is a real, parseable
  function call, dbt can statically scan the whole `models/` directory
  and build the dependency graph from it — that's how it knows
  `fct_trips` must run after `stg_yellow_tripdata`. `sources.yml`'s
  `{{ source('raw', 'yellow_tripdata') }}` does the same thing for raw
  tables (see `stg_yellow_tripdata.sql`).

Want to see what a model actually compiles to? Run `dbt compile` and
open the matching file under `target/compiled/nyc_taxi_dbt/models/` —
`{{ ref(...) }}` will have become a literal table reference. That
side-by-side (template vs. compiled SQL) is the fastest way to get an
intuition for what Jinja is doing.

This project only uses `ref()`/`source()` — the two constructs that
cover most day-to-day dbt work. Jinja's other features (`{% if %}`,
`{% for %}`, custom macros, `{{ var(...) }}`) are worth learning once a
model actually needs them — e.g. looping over columns, or an
incremental model's `{% if is_incremental() %}` block — rather than as
syntax to memorize up front.

## View vs. table (and why `dbt run` re-reads everything)

- **`view`**: dbt stores your `SELECT` as a database view. Nothing is
  copied locally — every query against it re-runs the SQL and, for
  `stg_yellow_tripdata`, re-fetches the remote parquet over HTTPS each
  time.
- **`table`**: dbt materializes the result into DuckDB on disk
  (`CREATE OR REPLACE TABLE ... AS SELECT`). The remote parquet is read
  once at build time; everything downstream reads the local table.

**`dbt run` does not diff or check for existing data — it always
rebuilds.** Every run re-executes each model's `SELECT` from scratch,
so a `table` model still re-fetches all remote parquet files on every
`dbt run`, same as a `view` would. Materializing as `table` only saves
the network round-trip *between* runs (downstream models read local
disk instead of the network); it doesn't make a single `dbt run` any
smarter.

If re-reading the full remote source on every run is too slow/costly:

- **Scope your runs** once staging is built, e.g.
  `dbt run --select fct_trips+` or `dbt run --exclude stg_yellow_tripdata`,
  so you only rebuild the models that actually changed.
- **Use `incremental` materialization** for models where you can
  express "only the new rows" in SQL (via `{% if is_incremental() %}`,
  filtering on a date or ID column) — the first run does a full load,
  later runs only add rows matching that filter. This doesn't avoid
  reading the remote file if the filter can't be pushed down to
  CloudFront/S3, but it avoids re-writing rows that are already local.

For this project's static TLC parquet source, the simplest habit is:
build `stg_yellow_tripdata` once, then scope subsequent runs to
`marts` so the remote fetch isn't repeated unnecessarily.

## Class-by-class build-up

- **Class 1:** `stg_yellow_tripdata` only — a `source()` reading raw
  parquet, cast/renamed columns, a `where` filter dropping bad rows.
  Materialized as a `table`.
- **Class 2:** column tests on staging (`not_null` on
  `pickup_at`, `trip_distance_miles`) plus a singular test,
  `assert_no_zero_or_null_trips.sql`. Adds `fct_trips`
  (`models/marts/fct_trips.sql`), wired via `{{ ref() }}` to the
  staging model — introduces model lineage, derived columns
  (duration/speed/tip%), and `table` materialization. A second
  singular test, `assert_no_unrealistic_trip_speeds.sql`, checks a
  *derived* column against a business rule (no NYC taxi can plausibly
  average >60mph) — something a generic `not_null`/`unique` test
  can't express.
- **Class 3:** the marts layer — aggregation models built on `fct_trips`
  (hourly/monthly demand, speed by hour and day of week, location
  rankings, and a daily summary), with `unique` / `not_null` /
  `dbt_utils.unique_combination_of_columns` tests on each grain.

## Model chain (end state, after Class 3)

`stg_yellow_tripdata` (table, cleans/casts raw parquet) →
`fct_trips` (table, adds duration/speed/tip_pct, drops bad durations) →
the marts (aggregates for reporting; `mart_location_rankings` also joins
`stg_taxi_zones`, a table caching the zone-lookup CSV)
