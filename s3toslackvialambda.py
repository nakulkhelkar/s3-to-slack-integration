

import json
import urllib3
import boto3
from datetime import datetime

# Slack webhook URL
slack_webhook_url = "https://hooks.slack.com/services/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"
# S3 bucket name
bucket_name = "mydumpydata"

def lambda_handler(event, context):
    http = urllib3.PoolManager()
    s3 = boto3.client("s3")
    current_date = datetime.now().strftime("%Y%m%d")
    folder_name = current_date + "/"
    message = ""

    try:
        s3.head_object(Bucket=bucket_name, Key=folder_name)
        message = f"Folder {folder_name} exists in the S3 bucket."
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if 'Contents' in response:
            num_files = len(response['Contents']) - 1
            if num_files == 0:
                message += " The folder is empty."
            else:
                message += f" The folder contains {num_files} file(s)."
        else:
            message += " The folder is empty."
    except s3.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            message = f"Folder {folder_name} does not exist in the S3 bucket."
        elif e.response["Error"]["Code"] == "NoSuchBucket":
            message = f"Bucket {bucket_name} does not exist."
        else:
            message = f"Error checking folder in S3 bucket: {str(e)}"

    data = {"text": message}

    r = http.request(
        "POST",
        slack_webhook_url,
        body=json.dumps(data),
        headers={"Content-Type": "application/json"}
    )

    if r.status == 200:
        print("Slack message sent successfully")
    else:
        print(f"Failed to send Slack message: {r.status}")

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
