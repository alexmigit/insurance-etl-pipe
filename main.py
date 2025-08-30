from extract.extract import extract_claims_from_csv
from transform.transform import transform_claims
from load.load import load_to_snowflake
from utils.logger import logger
from utils.notify import send_slack_notification

def main():
    """
    Main function to execute the ETL pipeline.
    It extracts data from a CSV file, transforms it, and loads it into Snowflake.
    """
    try:
        logger.info("Starting ETL pipeline...")
        send_slack_notification(":repeat: Insurance ETL pipeline started.")

        # Extract data from CSV
        df_raw = extract_claims_from_csv("data/claims.csv")
        logger.info(f"Extracted {len(df_raw)} records.")

        # Transform data
        transformed_df = transform_claims(df_raw)
        logger.info("Data transformation completed successfully.")

        # Load data to Snowflake        
        load_to_snowflake(transformed_df, "raw_claims")
        logger.info("Data loaded to Snowflake successfully.")

        # Send notification to Slack
        send_slack_notification("✅ Insurance ETL pipeline executed successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        # Send notification to Slack in case of failure
        send_slack_notification(f"🔴 ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()  
