import os
import tempfile
import pandas as pd
from config.config import config
from datetime import datetime
from load.load_utils import get_connection

def load_agent(df: pd.DataFrame, target_table: str):
    conn = get_connection()
    cursor = conn.cursor()

    # ✅ Validation checks
    required_cols = [
        "AGENT_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", "AGENCY_NAME"
    ]

    if df.empty:
        raise ValueError("❌ Agent DataFrame is empty — no rows to load into Snowflake.")

    if "AGENT_ID" not in df.columns:
        raise ValueError("❌ Agent DataFrame must include AGENT_ID column for merge key.")

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠️ Warning: Missing expected columns: {missing}")

    # Add load timestamp safely
    df = df.copy()
    df["LOAD_TS"] = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    print("✅ Agent DataFrame ready with columns:", df.columns.tolist())
    print("Sample data:\n", df.head(5))

    # Create target table if not exists
    create_target = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        AGENT_ID VARCHAR(12) PRIMARY KEY,
        FIRST_NAME VARCHAR(50),
        LAST_NAME VARCHAR(50),
        EMAIL VARCHAR(255),
        PHONE VARCHAR(20),
        AGENCY_NAME VARCHAR(100),
        LOAD_TS TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
    )
    """
    cursor.execute(create_target)

    # Create staging table
    staging_table = f"{target_table}_STAGING"
    cursor.execute(f"CREATE OR REPLACE TEMP TABLE {staging_table} LIKE {target_table}")

    # Save DataFrame to temp CSV (with headers)
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(tmp_file.name, index=False, header=True)
    tmp_file.close()

    stage_name = f"{target_table}_STAGE"
    cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")

    # PUT + COPY into staging (explicit column mapping)
    cursor.execute(f"PUT file://{tmp_file.name} @{stage_name} OVERWRITE=TRUE")
    cursor.execute(f"""
        COPY INTO {staging_table}
        FROM (
            SELECT
                $1::VARCHAR AS AGENT_ID,
                $2::VARCHAR AS FIRST_NAME,
                $3::VARCHAR AS LAST_NAME,
                $4::VARCHAR AS EMAIL,
                $5::VARCHAR AS PHONE,
                $6::VARCHAR AS AGENCY_NAME,
                $7::TIMESTAMP_NTZ AS LOAD_TS
            FROM @{stage_name}/{os.path.basename(tmp_file.name)}
        )
        FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE'
    """)

    # Debug check: ensure rows landed in staging
    cursor.execute(f"SELECT COUNT(*), COUNT(AGENT_ID) FROM {staging_table}")
    print("📊 Row counts in staging (total, with_agent_id):", cursor.fetchone())

    # MERGE staging → target (idempotent upsert) with row count
    merge_query = f"""
    MERGE INTO {target_table} t
    USING {staging_table} s
    ON t.AGENT_ID = s.AGENT_ID
    WHEN MATCHED THEN UPDATE SET
        FIRST_NAME = s.FIRST_NAME,
        LAST_NAME = s.LAST_NAME,
        EMAIL = s.EMAIL,
        PHONE = s.PHONE,
        AGENCY_NAME = s.AGENCY_NAME,
        LOAD_TS = s.LOAD_TS
    WHEN NOT MATCHED THEN INSERT (
        AGENT_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE, AGENCY_NAME, LOAD_TS)
    VALUES (
        s.AGENT_ID, s.FIRST_NAME, s.LAST_NAME, s.EMAIL, s.PHONE, s.AGENCY_NAME, s.LOAD_TS)
    """
    cursor.execute(merge_query)

    # Fetch row counts from Snowflake metadata
    merge_result = cursor.fetchone()
    if merge_result:
        print("✅ Merge result stats:", merge_result)

    # Cleanup
    cursor.execute(f"REMOVE @{stage_name}")
    os.unlink(tmp_file.name)

    cursor.close()
    print(f"🎉 Finished upsert into {target_table}")
