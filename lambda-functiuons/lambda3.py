import json
import boto3
import logging

glue_client = boto3.client('glue')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Received Success Event from EventBridge for Glue Job 2:")
    logger.info(json.dumps(event))
    
    # Target our upcoming masking job
    TARGET_GLUE_JOB = "glue-job-3"
    
    try:
        source_job = event['detail']['jobName']
        source_run_id = event['detail']['jobRunId']
        
        logger.info(f"Glue Job 2 ('{source_job}') succeeded. Run ID: {source_run_id}")
        logger.info(f"🚀 Triggering Security Masking Layer: Starting '{TARGET_GLUE_JOB}'...")
        
        # Call AWS Glue API
        response = glue_client.start_job_run(JobName=TARGET_GLUE_JOB)
        new_job_run_id = response['JobRunId']
        
        logger.info(f"✅ Successfully started {TARGET_GLUE_JOB}. Run ID: {new_job_run_id}")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Glue Job 3 Kicked off successfully', 'JobRunId': new_job_run_id})
        }
    except Exception as e:
        logger.error(f"❌ Error triggering Glue Job 3: {str(e)}")
        raise e