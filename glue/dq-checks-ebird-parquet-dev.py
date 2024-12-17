import sys
import awswrangler as wr

NULL_HOWMANY = """
SELECT 
    SUM(CASE WHEN howmany IS NULL THEN 1 ELSE 0 END) AS flag_count
FROM "ebird-db"."dev_ebird_ingest"
;
"""

NULL_LAT_LNG = """
SELECT 
    SUM(CASE WHEN lat IS NULL OR lng IS NULL THEN 1 ELSE 0 END) AS flag_count
FROM "ebird-db"."dev_ebird_ingest"
;
"""

INVALID_LAT_LNG = """
SELECT 
    SUM(CASE WHEN lat < -90 OR lat > 90 OR lng < -180 OR lng > 180 THEN 1 ELSE 0 END) AS flag_count
FROM "ebird-db"."dev_ebird_ingest"
;
"""

NULL_SPECIES_CODE = """
SELECT 
    SUM(CASE WHEN speciesCode IS NULL OR TRIM(speciesCode) = '' THEN 1 ELSE 0 END) AS flag_count
FROM "ebird-db"."dev_ebird_ingest"
;
"""

# Dictionary of checks to run
checks = {
    "NULL_HOWMANY": NULL_HOWMANY,
    "NULL_LAT_LNG": NULL_LAT_LNG,
    "INVALID_LAT_LNG": INVALID_LAT_LNG,
    "NULL_SPECIES_CODE": NULL_SPECIES_CODE,
}

# Run each check and exit if any fail
for check_name, query in checks.items():
    df = wr.athena.read_sql_query(sql=query, database="ebird-db")
    if df["flag_count"][0] > 0:
        sys.exit(
            f"Data Quality check failed for {check_name}. Found {df['flag_count'][0]} invalid rows."
        )
    else:
        print(f"{check_name} Quality check passed.")

print("All data quality checks passed successfully!")
