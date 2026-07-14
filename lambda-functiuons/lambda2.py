import json
import boto3
import os
import logging

# Initialize the Glue client and logger
glue_client = boto3.client('glue')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    # 1. Log the incoming EventBridge success payload for debugging
    logger.info("Received Success Event from EventBridge for Glue Job 1:")
    logger.info(json.dumps(event))
    
    # 2. Target Glue Job 2 name - reads from env var, falls back to hardcoded default
    TARGET_GLUE_JOB = os.environ.get('GLUE_JOB_NAME', 'glue-job2')
    
    try:
        # Extract metadata from Glue Job 1's event for tracking
        source_job = event['detail']['jobName']
        source_run_id = event['detail']['jobRunId']
        
        logger.info(f"Glue Job 1 ('{source_job}') succeeded. Run ID: {source_run_id}")
        logger.info(f"🚀 Triggering next pipeline stage: Starting '{TARGET_GLUE_JOB}'...")
        
        # 3. Call the AWS Glue API to start the second job
        response = glue_client.start_job_run(
            JobName=TARGET_GLUE_JOB
        )
        
        new_job_run_id = response['JobRunId']
        logger.info(f"✅ Successfully started {TARGET_GLUE_JOB}. New Job Run ID: {new_job_run_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully kicked off {TARGET_GLUE_JOB}',
                'JobRunId': new_job_run_id
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Error trying to start Glue Job 2: {str(e)}")
        raise e