import os
from flask import Flask, request, jsonify
import boto3
from botocore.exceptions import ClientError
import logging
from datetime import datetime

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize CloudWatch Logs client
cloudwatch_logs = boto3.client('logs')

# CloudWatch Log Group and Stream names
LOG_GROUP_NAME = '/flask-app/batch-logs'
LOG_STREAM_NAME = 'batch-endpoint-logs'

def ensure_log_group_and_stream():
    """Ensure the log group and stream exist."""
    try:
        cloudwatch_logs.create_log_group(logGroupName=LOG_GROUP_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
            logger.error(f"Failed to create log group: {e}")
            return False

    try:
        cloudwatch_logs.create_log_stream(logGroupName=LOG_GROUP_NAME, logStreamName=LOG_STREAM_NAME)
    except ClientError as e:
        if e.response['Error']['Code'] != 'ResourceAlreadyExistsException':
            logger.error(f"Failed to create log stream: {e}")
            return False

    return True

def log_to_cloudwatch(message):
    """Log a message to CloudWatch Logs."""
    timestamp = int(datetime.utcnow().timestamp() * 1000)
    try:
        cloudwatch_logs.put_log_events(
            logGroupName=LOG_GROUP_NAME,
            logStreamName=LOG_STREAM_NAME,
            logEvents=[
                {
                    'timestamp': timestamp,
                    'message': message
                }
            ]
        )
    except ClientError as e:
        logger.error(f"Failed to log to CloudWatch: {e}")

@app.route('/batch', methods=['POST'])
def batch():
    data = request.json
    log_message = f"Received batch request: {data}"
    logger.info(log_message)
    log_to_cloudwatch(log_message)
    return jsonify({"status": "success", "message": "Batch request logged"}), 200

if __name__ == '__main__':
    if ensure_log_group_and_stream():
        app.run(debug=True)
    else:
        logger.error("Failed to set up CloudWatch logging. Exiting.")
