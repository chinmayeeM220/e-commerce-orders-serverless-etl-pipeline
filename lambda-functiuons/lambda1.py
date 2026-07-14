import json
import os
import boto3
import urllib.parse
import csv
import io

s3 = boto3.client('s3')
sns = boto3.client('sns')

RAW_ZONE_BUCKET = os.environ.get('RAW_ZONE_BUCKET', 'chinmayee-s3-raw-zone')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')  # required for failure alerts

REQUIRED_COLUMNS = ['order_id', 'customer_id', 'order_amount', 'quantity',
                     'order_date', 'shipping_date', 'delivery_date', 'order_status']


def is_csv_file(object_key):
    """File format check - only .csv is accepted."""
    return object_key.lower().endswith('.csv')


def publish_failure(subject, message):
    if not SNS_TOPIC_ARN:
        print(f"WARNING: SNS_TOPIC_ARN not set. Would have published: {subject} - {message}")
        return
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject[:100],
        Message=message
    )


def lambda_handler(event, context):
    """
    Triggered by SQS, which is triggered by an S3 'ObjectCreated' event
    when a file lands in the input bucket.

    Only file-level checks are applied here:
      1. File extension must be .csv
      2. File must be parseable as CSV
      3. Header must contain all required columns

    If all checks pass, the file is copied AS-IS to the raw zone bucket.
    If any check fails, no copy happens - an SNS alert is published instead.
    """
    results_summary = []

    for record in event.get('Records', []):
        bucket_name = None
        object_key = None
        try:
            body = json.loads(record['body'])
            s3_records = body.get('Records', [])
            if not s3_records:
                print(f"No S3 records found in SQS message: {body}")
                continue

            for s3_record in s3_records:
                bucket_name = s3_record['s3']['bucket']['name']
                object_key = urllib.parse.unquote_plus(s3_record['s3']['object']['key'])

                print(f"Processing s3://{bucket_name}/{object_key}")
                summary = process_file(bucket_name, object_key)
                results_summary.append(summary)

        except Exception as e:
            error_msg = f"Unexpected Lambda error for s3://{bucket_name}/{object_key}: {str(e)}"
            print(error_msg)
            publish_failure("ECommerce Pipeline: Lambda Error", error_msg)
            raise  # let SQS retry / route to DLQ

    return {
        'statusCode': 200,
        'body': json.dumps(results_summary)
    }


def process_file(bucket_name, object_key):
    # --- Check 1: File extension must be .csv ---
    if not is_csv_file(object_key):
        error_msg = (
            f"File rejected: s3://{bucket_name}/{object_key} is not a .csv file. "
            f"Only CSV format is accepted. File was NOT copied to raw zone."
        )
        print(error_msg)
        publish_failure("ECommerce Pipeline: Invalid File Format", error_msg)
        return {'source_file': f's3://{bucket_name}/{object_key}', 'status': 'REJECTED_INVALID_FORMAT'}

    try:
        # --- Fetch the file ---
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_bytes = response['Body'].read()

        # --- Check 2: File must be parseable as CSV (valid encoding/structure) ---
        try:
            csv_content = file_bytes.decode('utf-8')
        except UnicodeDecodeError as e:
            raise ValueError(f"File is not valid UTF-8 text / not a readable CSV: {str(e)}")

        reader = csv.DictReader(io.StringIO(csv_content))
        fieldnames = reader.fieldnames

        if not fieldnames:
            raise ValueError("CSV has no header row / could not be parsed")

        # Sanity check that it actually has at least one data row and columns parse consistently
        row_count = 0
        for row in reader:
            row_count += 1
            if None in row.values():
                # DictReader puts None as key for ragged rows (extra columns) -
                # a sign of malformed CSV structure
                raise ValueError(f"Malformed CSV structure detected at row {row_count} "
                                  f"(column count mismatch)")

        if row_count == 0:
            raise ValueError("CSV has a header but contains no data rows")

        # --- Check 3: Required columns must be present in header ---
        missing_columns = set(REQUIRED_COLUMNS) - set(fieldnames)
        if missing_columns:
            raise ValueError(f"CSV is missing required columns: {sorted(missing_columns)}")

        # --- All file-level checks passed: copy AS-IS to raw zone ---
        base_filename = os.path.basename(object_key)
        s3.put_object(
            Bucket=RAW_ZONE_BUCKET,
            Key=f'raw/{base_filename}',
            Body=file_bytes,
            ContentType='text/csv'
        )
        print(f"Copied s3://{bucket_name}/{object_key} -> s3://{RAW_ZONE_BUCKET}/raw/{base_filename}")

        return {
            'source_file': f's3://{bucket_name}/{object_key}',
            'status': 'COPIED_TO_RAW_ZONE',
            'destination': f's3://{RAW_ZONE_BUCKET}/raw/{base_filename}',
            'row_count': row_count
        }

    except Exception as e:
        # Any file-level failure -> NEVER copy to raw zone, alert via SNS instead
        error_msg = (
            f"File-level validation failed for s3://{bucket_name}/{object_key}: {str(e)}. "
            f"File was NOT copied to raw zone."
        )
        print(error_msg)
        publish_failure("ECommerce Pipeline: File Validation Failure", error_msg)
        raise  # bubble up so SQS message retries / goes to DLQ