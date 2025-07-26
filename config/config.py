from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Configuration dictionary for Snowflake and Slack
config = {
    "snowflake": {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    },
    "slack_webhook": os.getenv("SLACK_WEBHOOK_URL")
}