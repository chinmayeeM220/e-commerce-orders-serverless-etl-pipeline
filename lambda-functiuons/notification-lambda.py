import os
import json
import boto3

sns_client = boto3.client('sns')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

def lambda_handler(event, context):
    print("Received raw event: ", json.dumps(event))
    
    source = event.get('source')
    detail_type = event.get('detail-type')
    detail = event.get('detail', {})
    
    subject = "Data Pipeline Notification"
    message_body = ""

    # --- Case A: Event originates from AWS Lambda ---
    if source == "lambda":
        function_arn = detail.get('responseContext', {}).get('functionArn', 'Unknown Lambda')
        function_name = function_arn.split(':')[-1]
        condition = detail.get('condition') # 'Success' or 'Failure'
        
        subject = f"Lambda Alert: {function_name} execution {condition.upper()}"
        
        message_body = (
            f"=== AWS Lambda Execution Status ===\n"
            f"Function Name: {function_name}\n"
            f"Status       : {condition}\n"
            f"Function ARN : {function_arn}\n"
            f"Timestamp    : {event.get('time')}\n\n"
        )
        
        # Include technical details if it's a failure
        if condition == "Failure":
            response_payload = detail.get('responsePayload', {})
            message_body += f"Error Details:\n{json.dumps(response_payload, indent=2)}"
        else:
            message_body += "Execution completed successfully without errors."

    # --- Case B: Event originates from AWS Glue ---
    elif source == "aws.glue" and detail_type == "Glue Job State Change":
        job_name = detail.get('jobName', 'Unknown Glue Job')
        state = detail.get('state') # 'SUCCEEDED' or 'FAILED'
        job_run_id = detail.get('jobRunId')
        
        subject = f"Glue Alert: {job_name} has {state}"
        
        message_body = (
            f"=== AWS Glue Job Status Change ===\n"
            f"Job Name   : {job_name}\n"
            f"Status     : {state}\n"
            f"Job Run ID : {job_run_id}\n"
            f"Timestamp  : {event.get('time')}\n\n"
        )
        
        if state == "FAILED":
            error_message = detail.get('message', 'No direct error message provided in metadata.')
            message_body += f"Error Details:\n{error_message}"
        else:
            message_body += "ETL Job process completed successfully."
            
    # --- Case C: Fallback for unexpected payloads ---
    else:
        subject = "Pipeline Alert: Unknown Trigger Source"
        message_body = f"Received an unhandled event routing from EventBridge:\n{json.dumps(event, indent=2)}"

    # --- Send formatted alert to SNS ---
    if not SNS_TOPIC_ARN:
        print("CRITICAL: SNS_TOPIC_ARN environment variable is not defined.")
        return {'statusCode': 500, 'body': 'Configuration Error'}
        
    print(f"Publishing to SNS: {subject}")
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100], # SNS subjects strictly cap out at 100 characters
        Message=message_body
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Notification pushed successfully.')
    }
