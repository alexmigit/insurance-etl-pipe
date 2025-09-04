import os
import tempfile
import pandas as pd
from config.config import config
from datetime import datetime
from load.load_utils import get_connection

def load_policy(df: pd.DataFrame, target_table: str):
    conn = get_connection()
    cursor = conn.cursor()

    # ✅ Validation checks
    required_cols = [
        "POLICY_ID", "CUSTOMER_ID", "POLICY_TYPE", "EFFECTIVE_DATE", 
        "EXPIRATION_DATE", "PREMIUM_AMOUNT", "STATUS", "AGENT_ID"
    ]

    if df.empty:
        raise ValueError("❌ Policy DataFrame is empty — no rows to load into Snowflake.")

    if "POLICY_ID" not in df.columns:
        raise ValueError("❌ Policy DataFrame must include POLICY_ID column for merge key.")

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠️ Warning: Missing expected columns: {missing}")

    # Add load timestamp safely
    df = df.copy()
    df["LOAD_TS"] = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    print("✅ Policy DataFrame ready with columns:", df.columns.tolist())
    print("Sample data:\n", df.head(5))

    # Create target table if not exists
    create_target = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        POLICY_ID VARCHAR(10) PRIMARY KEY,
        CUSTOMER_ID VARCHAR(10),
        POLICY_TYPE VARCHAR(25),
        EFFECTIVE_DATE DATE,
        EXPIRATION_DATE DATE,
        PREMIUM_AMOUNT NUMBER(12,2),
        STATUS VARCHAR(25),
        AGENT_ID VARCHAR(10),
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
                $1::VARCHAR AS POLICY_ID,
                $2::VARCHAR AS CUSTOMER_ID,
                $3::VARCHAR AS POLICY_TYPE,
                $4::DATE AS EFFECTIVE_DATE,
                $5::DATE AS EXPIRATION_DATE,
                $6::NUMBER(12,2) AS PREMIUM_AMOUNT,
                $7::VARCHAR AS STATUS,
                $8::VARCHAR AS AGENT_ID,
                $9::TIMESTAMP_NTZ AS LOAD_TS
            FROM @{stage_name}/{os.path.basename(tmp_file.name)}
        )
        FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE'
    """)

    # Debug check: ensure rows landed in staging
    cursor.execute(f"SELECT COUNT(*), COUNT(POLICY_ID) FROM {staging_table}")
    print("📊 Row counts in staging (total, with_claim_id):", cursor.fetchone())

    # MERGE staging → target (idempotent upsert) with row count
    merge_query = f"""
    MERGE INTO {target_table} t
    USING {staging_table} s
    ON t.POLICY_ID = s.POLICY_ID
    WHEN MATCHED THEN UPDATE SET
        CUSTOMER_ID = s.CUSTOMER_ID,
        POLICY_TYPE = s.POLICY_TYPE,
        EFFECTIVE_DATE = s.EFFECTIVE_DATE,
        EXPIRATION_DATE = s.EXPIRATION_DATE,
        PREMIUM_AMOUNT = s.PREMIUM_AMOUNT,
        STATUS = s.STATUS,
        AGENT_ID = s.AGENT_ID,
        LOAD_TS = s.LOAD_TS
    WHEN NOT MATCHED THEN INSERT (
        POLICY_ID, CUSTOMER_ID, POLICY_TYPE, EFFECTIVE_DATE, EXPIRATION_DATE, 
        PREMIUM_AMOUNT, STATUS, AGENT_ID, LOAD_TS)
    VALUES (
        s.POLICY_ID, s.CUSTOMER_ID, s.POLICY_TYPE, s.EFFECTIVE_DATE, s.EXPIRATION_DATE, 
        s.PREMIUM_AMOUNT, s.STATUS, s.AGENT_ID, s.LOAD_TS)
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

