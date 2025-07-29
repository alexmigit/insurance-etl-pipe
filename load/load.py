import snowflake.connector
from config.config import config

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
    conn= get_connection()
    cursor = conn.cursor()

    # Create table if it does not exist
    columns = ", ".join([f"{col} STRING" for col in df.columns])
    create_table_query = (f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {columns}
        );   
    """)
    cursor.execute(create_table_query)              

    # Insert data into the table
    for _, row in df.iterrows():
        # Prepare the insert query with placeholders for each column
        # Using %s as a placeholder for each value to prevent SQL injection
        placeholders = ", ".join(["%s"] * len(row))
        # Construct the insert query using the table name and placeholders
        # The row values will be passed as a tuple to the execute method
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(insert_query, tuple(row))       

    cursor.close()
    conn.close()