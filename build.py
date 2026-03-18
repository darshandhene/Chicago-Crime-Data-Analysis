"""
build.py - ETL Pipeline
Chicago Crime + 311 Service Requests
IS455 - Database Design & Prototyping | Midterm Project

Research Questions:
    1. What are the most common crime types and their arrest rates?
    2. Does the city's 311 service request backlog correlate with crime volume on the same day?
    3. Which districts and location types have the highest crime concentration?
    4. How do daily crime counts vary by day of week or month?

Usage:
    python3 build.py

The pipeline is idempotent. Re-running rebuilds every derived table.
"""

import os

import duckdb


DB_PATH = "chicago_crime.duckdb"
RAW_DIR = "data/raw"
CURATED_DIR = "data/curated"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CURATED_DIR, exist_ok=True)

con = duckdb.connect(DB_PATH)
print(f"[build.py] Connected to {DB_PATH}")


def audit(label, table, key_col=None):
    """Print row count, null count, and duplicate count for a table."""
    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"\n  [AUDIT] {label} -> {table}")
    print(f"    rows         : {row_count:,}")
    if key_col:
        nulls = con.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {key_col} IS NULL"
        ).fetchone()[0]
        dupes = con.execute(
            f"SELECT COUNT(*) FROM ("
            f"SELECT {key_col}, COUNT(*) c FROM {table} GROUP BY {key_col} HAVING c > 1)"
        ).fetchone()[0]
        print(f"    null {key_col:12s}: {nulls:,}")
        print(f"    dupe {key_col:12s}: {dupes:,}")


def has_column(table, column_name):
    result = con.execute(
        f"SELECT COUNT(*) FROM pragma_table_info('{table}') WHERE name = ?",
        [column_name],
    ).fetchone()[0]
    return result > 0


def pick_expr(table, candidates, transform=None, default="NULL"):
    for column_name in candidates:
        if has_column(table, column_name):
            if transform:
                return transform(column_name)
            return column_name
    return default


def require_file(path):
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Required input file not found: {path}. "
            "Download the raw CSVs first, then rerun build.py."
        )


require_file(f"{RAW_DIR}/crimes.csv")
require_file(f"{RAW_DIR}/311_service_requests.csv")


print("\n[STEP 1] Loading RAW layer...")

con.execute(f"""
    CREATE OR REPLACE TABLE raw_crimes AS
    SELECT * FROM read_csv_auto('{RAW_DIR}/crimes.csv', ALL_VARCHAR=TRUE)
    WHERE TRY_CAST("Year" AS INTEGER) = 2023
""")

audit("Crimes (raw)", "raw_crimes", "id")
con.execute("COPY (SELECT * FROM dim_police_district_name) TO 'data/curated/dim_police_district_name.parquet' (FORMAT PARQUET)")

con.execute(f"""
    CREATE OR REPLACE TABLE raw_311 AS
    SELECT * FROM read_csv_auto('{RAW_DIR}/311_service_requests.csv', ALL_VARCHAR=TRUE)
    WHERE TRY_CAST(SUBSTR("created_date", 1, 4) AS INTEGER) = 2023
""")




raw_311_key = pick_expr(
    "raw_311",
    ["service_request_number", "sr_number", "request_id"],
    default="NULL",
)
audit("311 requests (raw)", "raw_311", raw_311_key if raw_311_key != "NULL" else None)
raw_311_rows = con.execute("SELECT COUNT(*) FROM raw_311").fetchone()[0]
if raw_311_rows == 0:
    raise ValueError(
        "raw_311 loaded zero rows. Re-download data/raw/311_service_requests.csv "
        "before running build.py again."
    )


print("\n[STEP 2] Building STAGING layer...")

con.execute("""
    CREATE OR REPLACE TABLE stg_crimes AS
    SELECT
        CAST(id                    AS BIGINT)    AS crime_id,
        CAST(date                  AS TIMESTAMP) AS event_ts,
        DATE_TRUNC('day', CAST(date AS TIMESTAMP))::DATE AS event_date,
        CAST(year                  AS INTEGER)   AS event_year,
        CAST(beat                  AS INTEGER)   AS beat,
        CAST(district              AS INTEGER)   AS district,
        TRIM(primary_type)                       AS crime_type,
        TRIM(description)                        AS description,
        TRIM(location_description)               AS location_desc,
        CAST(arrest                AS BOOLEAN)   AS arrest_made,
        CAST(domestic              AS BOOLEAN)   AS is_domestic,
        TRY_CAST(latitude          AS DOUBLE)    AS latitude,
        TRY_CAST(longitude         AS DOUBLE)    AS longitude
    FROM raw_crimes
    WHERE id IS NOT NULL
      AND date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) = 1
""")
audit("Crimes (staging)", "stg_crimes", "crime_id")

request_id_expr = pick_expr(
    "raw_311",
    ["service_request_number", "sr_number", "request_id"],
    lambda c: f"TRIM({c})",
    default=(
        "CONCAT('row_', ROW_NUMBER() OVER (ORDER BY "
        "COALESCE(creation_date, created_date, current_timestamp::VARCHAR)))"
    ),
)
created_ts_expr = pick_expr(
    "raw_311",
    ["creation_date", "created_date", "requested_datetime"],
    lambda c: f"TRY_CAST({c} AS TIMESTAMP)",
)
completed_ts_expr = pick_expr(
    "raw_311",
    ["completion_date", "closed_date", "completed_date"],
    lambda c: f"TRY_CAST({c} AS TIMESTAMP)",
)
status_expr = pick_expr(
    "raw_311",
    ["status", "request_status"],
    lambda c: f"TRIM({c})",
)
service_type_expr = pick_expr(
    "raw_311",
    ["type_of_service_request", "service_name", "service_request_type"],
    lambda c: f"TRIM({c})",
)
ward_expr = pick_expr(
    "raw_311",
    ["ward"],
    lambda c: f"TRY_CAST({c} AS INTEGER)",
)
community_area_expr = pick_expr(
    "raw_311",
    ["community_area"],
    lambda c: f"TRY_CAST({c} AS INTEGER)",
)

con.execute(f"""
    CREATE OR REPLACE TABLE stg_311 AS
    SELECT
        {request_id_expr}                                            AS request_id,
        {created_ts_expr}                                            AS created_ts,
        DATE_TRUNC('day', {created_ts_expr})::DATE                   AS request_date,
        {completed_ts_expr}                                          AS completed_ts,
        DATE_TRUNC('day', {completed_ts_expr})::DATE                 AS completed_date,
        {status_expr}                                                AS status,
        {service_type_expr}                                          AS service_type,
        {ward_expr}                                                  AS ward,
        {community_area_expr}                                        AS community_area,
        CASE
            WHEN {completed_ts_expr} IS NULL OR {created_ts_expr} IS NULL THEN NULL
            ELSE DATE_DIFF('day', DATE_TRUNC('day', {created_ts_expr})::DATE,
                                DATE_TRUNC('day', {completed_ts_expr})::DATE)
        END                                                          AS days_to_close
    FROM raw_311
    WHERE {created_ts_expr} IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {request_id_expr}
        ORDER BY {created_ts_expr} DESC
    ) = 1
""")
audit("311 requests (staging)", "stg_311", "request_id")


print("\n[STEP 3] Building CURATED dimension tables...")

con.execute("""
    CREATE OR REPLACE TABLE dim_crime_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY crime_type) AS crime_type_id,
        crime_type,
        COUNT(*)                                AS total_occurrences
    FROM stg_crimes
    WHERE crime_type IS NOT NULL
    GROUP BY crime_type
""")
audit("dim_crime_type", "dim_crime_type", "crime_type_id")

con.execute("""
    CREATE OR REPLACE TABLE dim_district AS
    SELECT
        district AS district_id,
        COUNT(*) AS crimes_in_district
    FROM stg_crimes
    WHERE district IS NOT NULL
    GROUP BY district
""")
audit("dim_district", "dim_district", "district_id")

con.execute("""
    CREATE OR REPLACE TABLE dim_location_type AS
    SELECT
        ROW_NUMBER() OVER (ORDER BY location_desc) AS location_type_id,
        location_desc,
        COUNT(*)                                   AS total_occurrences
    FROM stg_crimes
    WHERE location_desc IS NOT NULL
    GROUP BY location_desc
""")
audit("dim_location_type", "dim_location_type", "location_type_id")


print("\n[STEP 4] Building CURATED fact tables...")

pre_count = con.execute("SELECT COUNT(*) FROM stg_crimes").fetchone()[0]

con.execute("""
    CREATE OR REPLACE TABLE fact_crime_events AS
    SELECT
        c.crime_id,
        c.event_ts,
        c.event_date,
        c.event_year,
        c.crime_type,
        ct.crime_type_id,
        c.description,
        c.location_desc,
        lt.location_type_id,
        c.beat,
        c.district,
        c.arrest_made,
        c.is_domestic,
        c.latitude,
        c.longitude
    FROM stg_crimes c
    LEFT JOIN dim_crime_type ct
        ON c.crime_type = ct.crime_type
    LEFT JOIN dim_location_type lt
        ON c.location_desc = lt.location_desc
""")

post_count = con.execute("SELECT COUNT(*) FROM fact_crime_events").fetchone()[0]
print(f"\n  [CARDINALITY CHECK] pre-join: {pre_count:,} | post-join: {post_count:,}")
if pre_count != post_count:
    print("  WARNING: Row count changed - investigate join explosion.")
else:
    print("  OK: joins preserved the crime-event grain.")
audit("fact_crime_events", "fact_crime_events", "crime_id")

con.execute("""
    CREATE OR REPLACE TABLE fact_311_requests AS
    SELECT
        request_id,
        created_ts,
        request_date,
        completed_ts,
        completed_date,
        status,
        service_type,
        ward,
        community_area,
        days_to_close
    FROM stg_311
""")
audit("fact_311_requests", "fact_311_requests", "request_id")

con.execute("""
    CREATE OR REPLACE TABLE fact_311_daily AS
    WITH request_days AS (
        SELECT request_date AS activity_date, COUNT(*) AS opened_requests
        FROM fact_311_requests
        GROUP BY request_date
    ), close_days AS (
        SELECT completed_date AS activity_date, COUNT(*) AS closed_requests
        FROM fact_311_requests
        WHERE completed_date IS NOT NULL
        GROUP BY completed_date
    ), calendar AS (
        SELECT activity_date FROM request_days
        UNION
        SELECT activity_date FROM close_days
    ), merged AS (
        SELECT
            c.activity_date,
            COALESCE(r.opened_requests, 0) AS opened_requests,
            COALESCE(cl.closed_requests, 0) AS closed_requests
        FROM calendar c
        LEFT JOIN request_days r
            ON c.activity_date = r.activity_date
        LEFT JOIN close_days cl
            ON c.activity_date = cl.activity_date
    )
    SELECT
        activity_date,
        opened_requests,
        closed_requests,
        SUM(opened_requests - closed_requests) OVER (
            ORDER BY activity_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS backlog_end_of_day
    FROM merged
    ORDER BY activity_date
""")
audit("fact_311_daily", "fact_311_daily", "activity_date")


print("\n[STEP 5] Exporting curated tables to Parquet...")

for table in [
    "fact_crime_events",
    "fact_311_requests",
    "fact_311_daily",
    "dim_crime_type",
    "dim_district",
    "dim_location_type",
]:
    out_path = f"{CURATED_DIR}/{table}.parquet"
    con.execute(f"COPY {table} TO '{out_path}' (FORMAT PARQUET)")
    print(f"  Exported -> {out_path}")


print("\n[STEP 6] Writing run manifest...")

expected_manifest_cols = [
    "run_id",
    "run_ts",
    "raw_crimes_rows",
    "stg_crimes_rows",
    "raw_311_rows",
    "stg_311_rows",
    "fact_crime_rows",
    "fact_311_req_rows",
    "fact_311_daily_rows",
    "notes",
]

manifest_exists = con.execute("""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_name = 'run_manifest'
""").fetchone()[0]

if manifest_exists:
    current_manifest_cols = [
        row[0] for row in con.execute("DESCRIBE run_manifest").fetchall()
    ]
    if current_manifest_cols != expected_manifest_cols:
        con.execute("""
            CREATE OR REPLACE TABLE run_manifest_migrated AS
            SELECT
                run_id,
                run_ts,
                raw_crimes_rows,
                stg_crimes_rows,
                NULL::BIGINT AS raw_311_rows,
                NULL::BIGINT AS stg_311_rows,
                fact_crime_rows,
                NULL::BIGINT AS fact_311_req_rows,
                NULL::BIGINT AS fact_311_daily_rows,
                notes
            FROM run_manifest
        """)
        con.execute("DROP TABLE run_manifest")
        con.execute("ALTER TABLE run_manifest_migrated RENAME TO run_manifest")
else:
    con.execute("""
        CREATE TABLE run_manifest (
            run_id              INTEGER PRIMARY KEY,
            run_ts              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_crimes_rows     BIGINT,
            stg_crimes_rows     BIGINT,
            raw_311_rows        BIGINT,
            stg_311_rows        BIGINT,
            fact_crime_rows     BIGINT,
            fact_311_req_rows   BIGINT,
            fact_311_daily_rows BIGINT,
            notes               VARCHAR
        )
    """)

next_id = con.execute(
    "SELECT COALESCE(MAX(run_id), 0) + 1 FROM run_manifest"
).fetchone()[0]

con.execute(f"""
    INSERT INTO run_manifest (
        run_id,
        run_ts,
        raw_crimes_rows,
        stg_crimes_rows,
        raw_311_rows,
        stg_311_rows,
        fact_crime_rows,
        fact_311_req_rows,
        fact_311_daily_rows,
        notes
    ) VALUES (
        {next_id},
        CURRENT_TIMESTAMP,
        (SELECT COUNT(*) FROM raw_crimes),
        (SELECT COUNT(*) FROM stg_crimes),
        (SELECT COUNT(*) FROM raw_311),
        (SELECT COUNT(*) FROM stg_311),
        (SELECT COUNT(*) FROM fact_crime_events),
        (SELECT COUNT(*) FROM fact_311_requests),
        (SELECT COUNT(*) FROM fact_311_daily),
        'Automated build via build.py (crimes + 311)'
    )
""")

print(con.execute(
    "SELECT * FROM run_manifest ORDER BY run_id DESC LIMIT 3"
).df().to_string(index=False))

con.close()
print("\n[build.py] Pipeline complete.")
