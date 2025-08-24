import snowflake.connector
from config.config import config
from datetime import datetime

def get_connection():
    """
    Establish a connection to the Snowflake database using the configuration.
    
    Returns:
        snowflake.connector.SnowflakeConnection: A connection object to interact with Snowflake.
    """
    conn = snowflake.connector.connect(
        user=config["snowflake"]["user"],
        password=config["snowflake"]["password"],
        account=config["snowflake"]["account"],
        warehouse=config["snowflake"]["warehouse"],
        database=config["snowflake"]["database"],
        schema=config["snowflake"]["schema"],
        role=config["snowflake"]["role"]
    )
    return conn

def load_to_snowflake(df, table_name):
    conn = get_connection()
    cursor = conn.cursor()

    # Create table if it does not exist (adding LOAD_TS column)
    columns = ", ".join([f"{col} STRING" for col in df.columns])
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns},
            LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
        );
    """
    cursor.execute(create_table_query)

    # Insert data into the table
    for _, row in df.iterrows():
        # Add placeholders for DF columns + LOAD_TS
        placeholders = ", ".join(["%s"] * len(row)) + ", CURRENT_TIMESTAMP"
        
        # Construct insert query (DF columns only, LOAD_TS is auto-inserted)
        insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}, LOAD_TS) VALUES ({placeholders})"
        
        # Execute insert query with DF row values
        cursor.execute(insert_query, tuple(row))

    cursor.close()
    conn.close()
