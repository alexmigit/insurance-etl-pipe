import requests
from config.config import config

def send_slack_notification(message: str):
    """
    Send a notification message to a Slack channel using a webhook URL.
    
    Args:
        message (str): The message to send to the Slack channel.
    """
    payload = {"text": message}
    try:
        requests.post(config["slack_webhook"], json=payload)
    except Exception as e:
        print(f"Failed to send Slack message: {e}")
