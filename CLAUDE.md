# DS3022 Course Repository (Fall 2026)

This is a student-facing template repository for DS3022 Data Engineering. Students fork
this repo and work through it independently, so treat it as instructor-side: it's fine
to help fully with exercises, fill in worksheets, and reference answer keys directly.

## Structure

The course is organized into numbered modules, each covering a stage of the data
engineering pipeline:

- `01_data/` — data types, schemas, dataframes
- `02_query/` — querying, columnar file formats, DuckDB
- `03_transform/dbt/` — dbt project (staging → marts) on DuckDB
- `04_apis` - basic and authenticated API calls, parsing of JSON, and pagination
- `05_prefect` - prefect flows, tasks, etc.

The module structure and content are still actively evolving this semester — feel free
to propose structural changes, new modules, or reorganizations rather than treating the
current layout as fixed.

## Python environment

Students may use **either pipenv or uv**. Don't assume one — detect which is in use
before running Python/dbt commands:

- If a `Pipfile.lock` exists and/or `pipenv` is on PATH and has an environment for this
  project, use `pipenv shell` / `pipenv run`.
- If a `uv.lock` or `.venv` created by uv exists, use `uv run` / `uv sync`.
- Match whichever tool's lock/env files are actually present in the working repo rather
  than defaulting to one.
- The virtual environment will be found AND SHOULD BE ACTIVATED from the root directory
  of the project. Nowhere else. Do not create multiple virtual envs within subdirectories.

## dbt workflow (`03_transform/dbt`)

- Activate the detected venv tool first, then run standard dbt commands (`dbt run`,
  `dbt test`, `dbt build`) from `03_transform/dbt/`.
- `profiles.yml.example` is the template; each student's real `profiles.yml` is local
  and not committed.

## File handling

Don't commit generated or large files — warn before staging things like:

- `*.duckdb` database files
- `03_transform/dbt/target/`, `03_transform/dbt/logs/`
- generated plots/images produced by pipeline runs (e.g. `03_transform/trip_distance_cutoffs.png`,
  `03_transform/dbt/plots/`)

## Code preferences

- Prefer Brevity and Concision over Verbosity
- Always enable error handling (try/except/etc.) and logging to local files.
- Keep python to single file implementations unless a separate class is required. Ask before creating a separate class file.
- Always comment functions or important stanzas of code to explain them to others.
