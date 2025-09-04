import os
import tempfile
import pandas as pd
from config.config import config
from datetime import datetime
from load.load_utils import get_connection

def load_customer(df: pd.DataFrame, target_table: str):
    conn = get_connection()
    cursor = conn.cursor()

    # ✅ Validation checks
    required_cols = [
        "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "DATE_OF_BIRTH", "GENDER",
        "EMAIL", "PHONE", "ADDRESS", "CITY", "STATE", "ZIP_CODE"
    ]

    if df.empty:
        raise ValueError("❌ Customer DataFrame is empty — no rows to load into Snowflake.")

    if "CUSTOMER_ID" not in df.columns:
        raise ValueError("❌ Customer DataFrame must include CUSTOMER_ID column for merge key.")

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"⚠️ Warning: Missing expected columns: {missing}")

    # Add load timestamp safely
    df = df.copy()
    df["LOAD_TS"] = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    print("✅ Customer DataFrame ready with columns:", df.columns.tolist())
    print("Sample data:\n", df.head(5))

    # Create target table if not exists
    create_target = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        CUSTOMER_ID VARCHAR(12) PRIMARY KEY,
        FIRST_NAME VARCHAR(50),
        LAST_NAME VARCHAR(50),
        DATE_OF_BIRTH DATE,
        GENDER VARCHAR(1),
        EMAIL VARCHAR(255),
        PHONE VARCHAR(20),
        ADDRESS VARCHAR(100),
        CITY VARCHAR(50),
        STATE VARCHAR(2),
        ZIP_CODE VARCHAR(5),
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
                $1::VARCHAR AS CUSTOMER_ID,
                $2::VARCHAR AS FIRST_NAME,
                $3::VARCHAR AS LAST_NAME,
                $4::DATE AS DATE_OF_BIRTH,
                $5::VARCHAR AS GENDER,
                $6::VARCHAR AS EMAIL,
                $7::VARCHAR AS PHONE,
                $8::VARCHAR AS ADDRESS,
                $9::VARCHAR AS CITY,
                $10::VARCHAR AS STATE,
                $11::VARCHAR AS ZIP_CODE,
                $12::TIMESTAMP_NTZ AS LOAD_TS
            FROM @{stage_name}/{os.path.basename(tmp_file.name)}
        )
        FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
        ON_ERROR='CONTINUE'
    """)

    # Debug check: ensure rows landed in staging
    cursor.execute(f"SELECT COUNT(*), COUNT(CUSTOMER_ID) FROM {staging_table}")
    print("📊 Row counts in staging (total, with_customer_id):", cursor.fetchone())

    # MERGE staging → target (idempotent upsert) with row count
    merge_query = f"""
    MERGE INTO {target_table} t
    USING {staging_table} s
    ON t.CUSTOMER_ID = s.CUSTOMER_ID
    WHEN MATCHED THEN UPDATE SET
        FIRST_NAME = s.FIRST_NAME,
        LAST_NAME = s.LAST_NAME,
        DATE_OF_BIRTH = s.DATE_OF_BIRTH,
        GENDER = s.GENDER,
        EMAIL = s.EMAIL,
        PHONE = s.PHONE,
        ADDRESS = s.ADDRESS,
        CITY = s.CITY,
        STATE = s.STATE,
        ZIP_CODE = s.ZIP_CODE,
        LOAD_TS = s.LOAD_TS
    WHEN NOT MATCHED THEN INSERT (
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, DATE_OF_BIRTH, GENDER, 
        EMAIL, PHONE, ADDRESS, CITY, STATE, ZIP_CODE, LOAD_TS)
    VALUES (
        s.CUSTOMER_ID, s.FIRST_NAME, s.LAST_NAME, s.DATE_OF_BIRTH, s.GENDER, 
        s.EMAIL, s.PHONE, s.ADDRESS, s.CITY, s.STATE, s.ZIP_CODE, s.LOAD_TS)
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

