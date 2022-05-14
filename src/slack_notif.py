#!/usr/bin/env python3

import logging
logging.basicConfig(level=logging.ERROR)

from dotenv import dotenv_values

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

"""
Code to send messages to Slack
"""

# Slack Token
SLACK_API_TOKEN = dotenv_values(".env")["SLACK_API_TOKEN"]

# Output channel on Slack
CHANNEL_ID = dotenv_values(".env")["CHANNEL_ID"]

def push_slack(msg):
    """ Function to push to Slack
    """

    # Create Slack client
    slack_client = WebClient(token=SLACK_API_TOKEN)

    # Post initial message
    try:
        response = slack_client.chat_postMessage(
                channel=CHANNEL_ID,
                text=msg)
    except SlackApiError as e:
        assert e.response["ok"] is False
        # assert exception.response["error"]
        # print(f"Slack error: {exception.response['error']}")

