# Databricks notebook source
# =============================================================================
# Schema Registry — Populate & Track Schema Changes
#
# Monitors schema changes across all dev_* catalogs and records them
# in dev_schema_registry.
#
# Detection approach:
#   1. For every table: compare current schema_json vs stored snapshot
#   2. If schemas differ → check DESCRIBE HISTORY to classify the change
#   3. Catches: ADD/DROP/RENAME/ALTER COLUMN, MERGE, STREAMING UPDATE,
#      WRITE with mergeSchema
#
# Key design:
#   - Auto-discovers all catalogs starting with 'dev_'
#   - Scans ALL schemas in a catalog (excluding information_schema, observability)
#   - Time-bound: only checks tables modified since last scan (via run_config)
#   - DESCRIBE HISTORY only called when a schema change is detected (efficient)
#   - Parallel catalog processing with batch Delta writes (no write conflicts)
#   - New tables get INITIAL REGISTRATION (version 1, no history audit)
# =============================================================================


# =============================================================================
# CONFIGURATION
# =============================================================================
import json
import time
from datetime import datetime, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

REGISTRY_CATALOG = "dev_dlh_platform"
REGISTRY_SCHEMA  = "observability"
REGISTRY_TABLE   = f"{REGISTRY_CATALOG}.{REGISTRY_SCHEMA}.dev_schema_registry"
RUN_CONFIG_TABLE = f"{REGISTRY_CATALOG}.{REGISTRY_SCHEMA}.dev_schema_monitoring_run_config"

EXCLUDED_SCHEMAS = {"information_schema", "observability"}

TRACKED_OPERATIONS = {
    "ADD COLUMNS", "DROP COLUMNS", "RENAME COLUMN", "ALTER COLUMN",
    "MERGE", "STREAMING UPDATE",
}

# Discover all catalogs starting with 'dev_'
all_catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
CATALOGS_TO_MONITOR = sorted([c for c in all_catalogs if c.startswith("dev_")])

print(f"Registry   : {REGISTRY_TABLE}")
print(f"Run Config : {RUN_CONFIG_TABLE}")
print(f"Catalogs   : {CATALOGS_TO_MONITOR} ({len(CATALOGS_TO_MONITOR)} total)")


# =============================================================================
# CREATE / VERIFY TABLES
# =============================================================================

# -- Observability schema --
try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {REGISTRY_CATALOG}.{REGISTRY_SCHEMA}")
    print(f"Schema '{REGISTRY_CATALOG}.{REGISTRY_SCHEMA}' is ready.")
except Exception as e:
    if "PERMISSION_DENIED" in str(e) or "UNAUTHORIZED_ACCESS" in str(e):
        print(f"Schema already exists (skipped due to permissions).")
    else:
        raise

# -- dev_schema_registry --
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {REGISTRY_TABLE} (
        id                          BIGINT GENERATED ALWAYS AS IDENTITY,
        catalog_name                VARCHAR(255),
        schema_name                 VARCHAR(255),
        table_name                  VARCHAR(255),
        schema_version              INT,
        created_by                  VARCHAR(255),
        modified_by                 VARCHAR(255),
        modified_timestamp          TIMESTAMP,
        schema_json                 STRING,
        change_type                 VARCHAR(50),
        column_name                 VARCHAR(255),
        table_version               INT,
        table_version_timestamp     TIMESTAMP,
        status                      VARCHAR(50),
        check_timestamp             TIMESTAMP,
        ddl_snapshot                STRING
    )
""")
print(f"Table '{REGISTRY_TABLE}' is ready.")

# -- Run config: tracks last scan time per catalog --
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {RUN_CONFIG_TABLE} (
        catalog_name        VARCHAR(255) NOT NULL,
        last_scan_timestamp TIMESTAMP
    )
""")

# Seed any new catalogs that aren't in run_config yet
for cat in CATALOGS_TO_MONITOR:
    exists = spark.sql(f"SELECT 1 FROM {RUN_CONFIG_TABLE} WHERE catalog_name = '{cat}'").count()
    if exists == 0:
        spark.sql(f"INSERT INTO {RUN_CONFIG_TABLE} VALUES ('{cat}', NULL)")
        print(f"  Seeded run_config for '{cat}' (first run — will scan all tables)")

print(f"Table '{RUN_CONFIG_TABLE}' is ready.")

# -- Schema definition for DataFrame creation (excludes identity column) --
REGISTRY_SCHEMA_DEF = StructType([
    StructField("catalog_name",                StringType(),    False),
    StructField("schema_name",                 StringType(),    False),
    StructField("table_name",                  StringType(),    False),
    StructField("schema_version",              IntegerType(),   False),
    StructField("created_by",                  StringType(),    True),
    StructField("modified_by",                 StringType(),    True),
    StructField("modified_timestamp",          TimestampType(), True),
    StructField("schema_json",                 StringType(),    True),
    StructField("change_type",                 StringType(),    True),
    StructField("column_name",                 StringType(),    True),
    StructField("table_version",               IntegerType(),   True),
    StructField("table_version_timestamp",     TimestampType(), True),
    StructField("status",                      StringType(),    False),
    StructField("check_timestamp",             TimestampType(), False),
    StructField("ddl_snapshot",                StringType(),    True),
])


# =============================================================================
# HELPER FUNCTIONS & CHANGE DETECTION
# =============================================================================

# ── Run Config helpers ──────────────────────────────────────────────────────

def get_last_scan_time(catalog):
    """Read last_scan_timestamp from run_config. Returns None if first run."""
    rows = spark.sql(
        f"SELECT last_scan_timestamp FROM {RUN_CONFIG_TABLE} WHERE catalog_name = '{catalog}'"
    ).collect()
    if rows and rows[0]["last_scan_timestamp"]:
        return rows[0]["last_scan_timestamp"]
    return None


def update_last_scan_time(catalog):
    """Update run_config with current timestamp after successful scan."""
    spark.sql(f"""
        UPDATE {RUN_CONFIG_TABLE}
        SET last_scan_timestamp = current_timestamp()
        WHERE catalog_name = '{catalog}'
    """)


# ── Time-bound table discovery ───────────────────────────────────────────

def get_all_eligible_tables(catalog):
    """All managed/external tables across all eligible schemas."""
    rows = spark.sql(f"""
        SELECT table_schema, table_name
        FROM `{catalog}`.information_schema.tables
        WHERE table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE')
          AND table_name NOT LIKE '__apply%'
    """).collect()
    return [(r["table_schema"], r["table_name"]) for r in rows
            if r["table_schema"] not in EXCLUDED_SCHEMAS]


def get_modified_tables_since(catalog, last_scan_ts):
    """
    Tables modified since last scan (from system.information_schema.tables.last_altered).
    Returns a set of (schema, table) tuples.
    """
    ts_str = last_scan_ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    rows = spark.sql(f"""
        SELECT table_schema, table_name
        FROM `{catalog}`.information_schema.tables
        WHERE last_altered > TIMESTAMP '{ts_str}'
          AND table_type IN ('MANAGED', 'EXTERNAL', 'BASE TABLE')
          AND table_name NOT LIKE '__apply%'
    """).collect()
    return {(r["table_schema"], r["table_name"]) for r in rows
            if r["table_schema"] not in EXCLUDED_SCHEMAS}


def get_registered_tables(catalog):
    """Set of (schema, table) tuples currently Active in registry."""
    rows = spark.sql(f"""
        SELECT schema_name, table_name FROM {REGISTRY_TABLE}
        WHERE catalog_name = '{catalog}' AND status = 'Active'
    """).collect()
    return {(r["schema_name"], r["table_name"]) for r in rows}


def get_tables_to_check(catalog, last_scan_ts):
    """
    Determine which tables need checking:
    - First run (last_scan_ts is None): ALL eligible tables
    - Subsequent runs: tables modified since last scan + unregistered tables
    """
    all_tables = get_all_eligible_tables(catalog)

    if last_scan_ts is None:
        return all_tables  # First run — scan everything

    modified = get_modified_tables_since(catalog, last_scan_ts)
    registered = get_registered_tables(catalog)

    # Include modified tables + any new tables not yet in registry
    return [(s, t) for s, t in all_tables
            if (s, t) in modified or (s, t) not in registered]


# ── Schema capture & comparison ─────────────────────────────────────────

def get_current_schema_json(catalog, schema, table):
    """Current schema as sorted JSON map {col: dtype}."""
    rows = spark.sql(
        f"SELECT column_name, data_type FROM `{catalog}`.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).collect()
    return json.dumps({r["column_name"]: r["data_type"] for r in rows}, sort_keys=True)


def get_current_ddl(catalog, schema, table):
    """Capture SHOW CREATE TABLE output."""
    try:
        return spark.sql(f"SHOW CREATE TABLE `{catalog}`.`{schema}`.`{table}`").collect()[0][0]
    except Exception:
        return None


def get_table_owner(catalog, schema, table):
    """Fetch table owner from information_schema."""
    rows = spark.sql(
        f"SELECT table_owner, created_by FROM `{catalog}`.information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    ).collect()
    if rows:
        return rows[0]["created_by"] or rows[0]["table_owner"] or "unknown"
    return "unknown"


def build_entry(cat, sch, tbl, version, created_by, modified_by, modified_ts,
                schema_json, ddl, change_type, column_name, table_version,
                table_version_ts, status):
    """Build a registry row dict."""
    return {
        "catalog_name": cat, "schema_name": sch, "table_name": tbl,
        "schema_version": version,
        "created_by": created_by, "modified_by": modified_by,
        "modified_timestamp": modified_ts,
        "schema_json": schema_json, "ddl_snapshot": ddl,
        "change_type": change_type, "column_name": column_name,
        "table_version": int(table_version) if table_version is not None else None,
        "table_version_timestamp": table_version_ts,
        "status": status,
        "check_timestamp": datetime.now(timezone.utc),
    }


def identify_change_type(catalog, schema, table, last_check):
    """
    DESCRIBE HISTORY to classify the schema change.
    Only called when schema_json differs (efficient).
    Returns: (change_type, modified_by, version, timestamp)
    """
    hist = spark.sql(f"DESCRIBE HISTORY `{catalog}`.`{schema}`.`{table}`").collect()

    for row in hist:
        if last_check and row["timestamp"] <= last_check:
            break
        op = row["operation"]
        if op in TRACKED_OPERATIONS:
            return op, row["userName"], row["version"], row["timestamp"]
        if op == "WRITE":
            return f"SCHEMA EVOLUTION ({op})", row["userName"], row["version"], row["timestamp"]

    return "SCHEMA EVOLUTION", "System", None, None


def process_table(catalog, schema, table):
    """
    Compare current schema against stored schema.
    Returns: (status, entry_to_write, key_to_deactivate)
      - status: 'registered' | 'changed' | 'unchanged'
      - entry_to_write: dict (for new/changed) or None (unchanged)
      - key_to_deactivate: (cat, sch, tbl) or None
    """
    owner = get_table_owner(catalog, schema, table)
    current_json = get_current_schema_json(catalog, schema, table)
    current_ddl  = get_current_ddl(catalog, schema, table)

    existing = spark.sql(
        f"SELECT * FROM {REGISTRY_TABLE} WHERE catalog_name='{catalog}' "
        f"AND schema_name='{schema}' AND table_name='{table}' AND status='Active'"
    ).collect()

    # -- NEW TABLE --
    if not existing:
        entry = build_entry(
            catalog, schema, table, 1, owner, "System",
            datetime.now(timezone.utc), current_json, current_ddl,
            "INITIAL REGISTRATION", "Table Level", None, None, "Active"
        )
        return "registered", entry, None

    # -- EXISTING: compare schema_json --
    latest_row = existing[0]
    if latest_row["schema_json"] == current_json:
        return "unchanged", None, None

    # -- SCHEMA CHANGED --
    last_check = latest_row["check_timestamp"]
    change_type, modified_by, tbl_version, tbl_ts = identify_change_type(
        catalog, schema, table, last_check
    )
    max_v = spark.sql(
        f"SELECT COALESCE(MAX(schema_version), 0) FROM {REGISTRY_TABLE} "
        f"WHERE catalog_name='{catalog}' AND schema_name='{schema}' AND table_name='{table}'"
    ).collect()[0][0]

    entry = build_entry(
        catalog, schema, table, int(max_v) + 1, owner, modified_by,
        tbl_ts or datetime.now(timezone.utc), current_json, current_ddl,
        change_type, "Table Level", tbl_version, tbl_ts, "Active"
    )
    return "changed", entry, (catalog, schema, table)


print("Helper functions defined.")


# =============================================================================
# MAIN ORCHESTRATOR — parallel catalogs, batch writes (no write conflicts)
# =============================================================================

def run_for_catalog(catalog_name):
    """
    Scan a single catalog. Returns entries to write and keys to deactivate.
    NO Delta writes happen here — everything is collected in memory.
    """
    t0 = time.time()
    counts = {"registered": 0, "changed": 0, "unchanged": 0, "skipped": 0, "error": 0}
    entries = []           # rows to INSERT
    deactivate_keys = []   # (cat, sch, tbl) to mark Inactive

    # -- Read last scan time --
    last_scan = get_last_scan_time(catalog_name)
    scan_mode = "FULL (first run)" if last_scan is None else f"INCREMENTAL (since {last_scan})"

    print(f"\n{'='*60}")
    print(f"Catalog: {catalog_name}  |  Mode: {scan_mode}")
    print(f"{'='*60}")

    # -- Get tables to check (time-bound) --
    all_tables = get_all_eligible_tables(catalog_name)
    tables_to_check = get_tables_to_check(catalog_name, last_scan)
    counts["skipped"] = len(all_tables) - len(tables_to_check)

    print(f"  Total eligible: {len(all_tables)} | To check: {len(tables_to_check)} | Skipped (unchanged): {counts['skipped']}")

    # -- Group by schema for display --
    by_schema = defaultdict(list)
    for sch, tbl in tables_to_check:
        by_schema[sch].append(tbl)

    for schema_name in sorted(by_schema):
        tables = by_schema[schema_name]
        print(f"\n--- {catalog_name}.{schema_name} ({len(tables)} tables) ---")

        for tbl in tables:
            try:
                status, entry, deact_key = process_table(catalog_name, schema_name, tbl)
                counts[status] += 1
                if entry:
                    entries.append(entry)
                if deact_key:
                    deactivate_keys.append(deact_key)
                if status == "registered":
                    print(f"  [NEW] {tbl}")
                elif status == "changed":
                    print(f"  [CHG] {tbl}")
            except Exception as e:
                counts["error"] += 1
                print(f"  [ERR] {tbl} \u2014 {e}")

    elapsed = time.time() - t0
    total = counts["registered"] + counts["changed"] + counts["unchanged"] + counts["error"]
    print(f"\n[DONE] {catalog_name}: {total} checked + {counts['skipped']} skipped in {elapsed:.1f}s")
    print(f"  {counts['registered']} new | {counts['changed']} changed | {counts['unchanged']} unchanged | {counts['skipped']} skipped | {counts['error']} errors")
    return counts, entries, deactivate_keys


# =============================================================================
# RUN ALL CATALOGS IN PARALLEL — collect in memory, then batch write
# =============================================================================
t_start = time.time()
all_counts = {"registered": 0, "changed": 0, "unchanged": 0, "skipped": 0, "error": 0}
all_entries = []
all_deactivate_keys = []
completed_catalogs = []

max_workers = min(len(CATALOGS_TO_MONITOR), 4)
print(f"Starting parallel scan of {len(CATALOGS_TO_MONITOR)} catalog(s) with {max_workers} threads...")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(run_for_catalog, cat): cat for cat in CATALOGS_TO_MONITOR}
    for future in as_completed(futures):
        cat = futures[future]
        try:
            c, entries, deact_keys = future.result()
            for k in all_counts:
                all_counts[k] += c[k]
            all_entries.extend(entries)
            all_deactivate_keys.extend(deact_keys)
            completed_catalogs.append(cat)
        except Exception as e:
            print(f"\n[FATAL] {cat} \u2014 {e}")
            all_counts["error"] += 1

# =============================================================================
# BATCH WRITE — single Delta transaction (no write conflicts)
# =============================================================================
print(f"\n--- Batch write: {len(all_entries)} entries, {len(all_deactivate_keys)} deactivations ---")

# 1. Deactivate old versions (single UPDATE)
if all_deactivate_keys:
    conditions = " OR ".join(
        f"(catalog_name='{c}' AND schema_name='{s}' AND table_name='{t}')"
        for c, s, t in all_deactivate_keys
    )
    spark.sql(f"""
        UPDATE {REGISTRY_TABLE} SET status = 'Inactive'
        WHERE ({conditions}) AND status = 'Active'
    """)
    print(f"  Deactivated {len(all_deactivate_keys)} old entries.")

# 2. Insert all new entries (single append)
if all_entries:
    df = spark.createDataFrame(all_entries, schema=REGISTRY_SCHEMA_DEF)
    df.write.format("delta").mode("append").saveAsTable(REGISTRY_TABLE)
    print(f"  Wrote {len(all_entries)} new entries.")

# 3. Update run_config for all completed catalogs
for cat in completed_catalogs:
    update_last_scan_time(cat)
print(f"  Updated run_config for {len(completed_catalogs)} catalog(s).")

total = sum(all_counts.values())
t_total = time.time() - t_start
print(f"\n{'='*60}")
print(f"ALL DONE \u2014 {len(CATALOGS_TO_MONITOR)} catalog(s), {total} tables in {t_total:.1f}s")
print(f"  {all_counts['registered']} new | {all_counts['changed']} changed | {all_counts['unchanged']} unchanged | {all_counts['skipped']} skipped | {all_counts['error']} errors")
print(f"{'='*60}")
