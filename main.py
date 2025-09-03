from extract.extract_claims import extract_claims
from extract.extract_policy import extract_policy
from transform.transform_claims import transform_claims
from transform.transform_policy import transform_policy
from load.load_claims import load_claims
from load.load_policy import load_policy
from utils.logger import logger
from utils.notify import send_slack_notification
#from data.gen_claims import generate_synthetic_claims

def main():
    """
    Main function to execute the ETL pipeline.
    It extracts data from a CSV file, transforms it, and loads it into Snowflake.
    """
    try:
        logger.info("Starting insurance ETL pipeline...")
        send_slack_notification(":repeat: Insurance ETL pipeline started.")

        # Generate synthetic claims data (if needed)
        #generate_synthetic_claims("data/claims.csv", num_new_claims=200, duplicate_rate=0.05)

        # Extract data from CSV files
        df_raw_claims = extract_claims("data/claims.csv")
        logger.info(f"Extracted {len(df_raw_claims)} claim records.")

        df_raw_policies = extract_policy("data/policies.csv")
        logger.info(f"Extracted {len(df_raw_policies)} policy records.")

        # Transform data
        transformed_df_claims = transform_claims(df_raw_claims)
        logger.info("Claims data transformation completed successfully.")

        transformed_df_policies = transform_policy(df_raw_policies)
        logger.info("Policy data transformation completed successfully.")

        # Load data to Snowflake        
        load_claims(transformed_df_claims, "RAW_CLAIM")
        logger.info("Claims data loaded to Snowflake successfully.")

        load_policy(transformed_df_policies, "RAW_POLICY")
        logger.info("Policy data loaded to Snowflake successfully.")

        # Log pipeline completion
        logger.info("Insurance ETL pipeline completed successfully.")

        # Send notification to Slack
        send_slack_notification("✅ Insurance ETL pipeline executed successfully.")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        # Send notification to Slack in case of failure
        send_slack_notification(f"🔴 Insurance ETL pipeline failed: {e}")

if __name__ == "__main__":
    main()  
