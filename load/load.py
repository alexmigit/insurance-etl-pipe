import os
import tempfile
import pandas as pd
import snowflake.connector
from config.config import config
from datetime import datetime

def get_connection():
    return snowflake.connector.connect(
        user=config["snowflake"]["user"],
        password=config["snowflake"]["password"],
        account=config["snowflake"]["account"],
        warehouse=config["snowflake"]["warehouse"],
        database=config["snowflake"]["database"],
        schema=config["snowflake"]["schema"],
        role=config["snowflake"]["role"]
    )

def load_to_snowflake(df: pd.DataFrame, target_table: str):
    conn = get_connection()
    cursor = conn.cursor()

    # Add load timestamp safely
    df = df.copy()
    df["LOAD_TS"] = datetime.utcnow().isoformat(sep=" ", timespec="seconds")

    # Create target table if not exists
    create_target = f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        CLAIM_ID STRING PRIMARY KEY,
        POLICY_ID STRING,
        CUSTOMER_ID STRING,
        CLAIM_AMOUNT STRING,
        CLAIM_DATE STRING,
        INCIDENT_DATE STRING,
        CLAIM_TYPE STRING,
        STATUS STRING,
        ADJUSTER_NOTES STRING,
        LOAD_TS TIMESTAMP_NTZ
    )
    """
    cursor.execute(create_target)

    # Create staging table
    staging_table = f"{target_table}_STAGING"
    cursor.execute(f"CREATE OR REPLACE TEMP TABLE {staging_table} LIKE {target_table}")

    # Save DataFrame to temp CSV
    tmp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.to_csv(tmp_file.name, index=False, header=False)

    stage_name = f"{target_table}_STAGE"
    cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")

    # PUT + COPY into staging
    cursor.execute(f"PUT file://{tmp_file.name} @{stage_name} OVERWRITE=TRUE")
    cursor.execute(f"""
        COPY INTO {staging_table}
        FROM @{stage_name}/{os.path.basename(tmp_file.name)}
        FILE_FORMAT = (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='"')
        ON_ERROR='CONTINUE'
    """)

    # MERGE staging → target (idempotent)
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
    WHEN NOT MATCHED THEN INSERT VALUES (
        s.CLAIM_ID, s.POLICY_ID, s.CUSTOMER_ID, s.CLAIM_AMOUNT, s.CLAIM_DATE,
        s.INCIDENT_DATE, s.CLAIM_TYPE, s.STATUS, s.ADJUSTER_NOTES, s.LOAD_TS
    )
    """
    cursor.execute(merge_query)

    # Cleanup
    cursor.execute(f"REMOVE @{stage_name}")
    tmp_file.close()
    os.unlink(tmp_file.name)

    cursor.close()
    conn.close()
    print(f"✅ Upserted {len(df)} rows into {target_table} (idempotent)")
