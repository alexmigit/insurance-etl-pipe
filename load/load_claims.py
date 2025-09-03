import os
import tempfile
import pandas as pd
import snowflake.connector
from config.config import config
from datetime import datetime
from load.load_utils import get_connection

def load_claims(df: pd.DataFrame, target_table: str):
    conn = get_connection()
    cursor = conn.cursor()

    # ✅ Validation checks
    required_cols = [
        "CLAIM_ID", "POLICY_ID", "CUSTOMER_ID", "CLAIM_AMOUNT",
        "CLAIM_DATE", "INCIDENT_DATE", "CLAIM_TYPE", "STATUS",
        "ADJUSTER_NOTES"
    ]

    if df.empty:
        raise ValueError("❌ DataFrame is empty — no rows to load into Snowflake.")

    if "CLAIM_ID" not in df.columns:
        raise ValueError("❌ DataFrame must include CLAIM_ID column for merge key.")

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠️ Warning: Missing expected columns: {missing}")

    # Add load timestamp safely
    df = df.copy()
    df["LOAD_TS"] = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    print("✅ DataFrame ready with columns:", df.columns.tolist())
    print("Sample data:\n", df.head(5))

    # Create target table if not exists
    create_target = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        CLAIM_ID VARCHAR(10) PRIMARY KEY,
        POLICY_ID VARCHAR(10),
        CUSTOMER_ID VARCHAR(10),
        CLAIM_AMOUNT NUMBER(12,2),
        CLAIM_DATE DATE,
        INCIDENT_DATE DATE,
        CLAIM_TYPE VARCHAR(25),
        STATUS VARCHAR(25),
        ADJUSTER_NOTES VARCHAR(1000),
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
                $1::VARCHAR AS CLAIM_ID,
                $2::VARCHAR AS POLICY_ID,
                $3::VARCHAR AS CUSTOMER_ID,
                $4::NUMBER(12,2) AS CLAIM_AMOUNT,
                $5::DATE AS CLAIM_DATE,
                $6::DATE AS INCIDENT_DATE,
                $7::VARCHAR AS CLAIM_TYPE,
                $8::VARCHAR AS STATUS,
                $9::VARCHAR AS ADJUSTER_NOTES,
                $10::TIMESTAMP_NTZ AS LOAD_TS
            FROM @{stage_name}/{os.path.basename(tmp_file.name)}
        )
        FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE'
    """)

    # Debug check: ensure rows landed in staging
    cursor.execute(f"SELECT COUNT(*), COUNT(CLAIM_ID) FROM {staging_table}")
    print("📊 Row counts in staging (total, with_claim_id):", cursor.fetchone())

    # MERGE staging → target (idempotent upsert) with row count
    merge_query = f"""
    MERGE INTO {target_table} t
    USING {staging_table} s
    ON t.CLAIM_ID = s.CLAIM_ID
    WHEN MATCHED THEN UPDATE SET
        POLICY_ID = s.POLICY_ID,
        CUSTOMER_ID = s.CUSTOMER_ID,
        CLAIM_AMOUNT = s.CLAIM_AMOUNT,
        CLAIM_DATE = s.CLAIM_DATE,
        INCIDENT_DATE = s.INCIDENT_DATE,
        CLAIM_TYPE = s.CLAIM_TYPE,
        STATUS = s.STATUS,
        ADJUSTER_NOTES = s.ADJUSTER_NOTES,
        LOAD_TS = s.LOAD_TS
    WHEN NOT MATCHED THEN INSERT (
        CLAIM_ID, POLICY_ID, CUSTOMER_ID, CLAIM_AMOUNT, CLAIM_DATE,
        INCIDENT_DATE, CLAIM_TYPE, STATUS, ADJUSTER_NOTES, LOAD_TS
    )
    VALUES (
        s.CLAIM_ID, s.POLICY_ID, s.CUSTOMER_ID, s.CLAIM_AMOUNT, s.CLAIM_DATE,
        s.INCIDENT_DATE, s.CLAIM_TYPE, s.STATUS, s.ADJUSTER_NOTES, s.LOAD_TS
    )
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
