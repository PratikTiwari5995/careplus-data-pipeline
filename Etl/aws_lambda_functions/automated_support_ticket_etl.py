import json
import boto3

# Initialize AWS Glue client
glue = boto3.client("glue")


def lambda_handler(event, context):
    """
    Trigger an AWS Glue ETL job when a file is uploaded to S3.
    Extracts the S3 path from the event and passes it to the Glue job.
    """

    # Extract bucket name and object key from the S3 event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    input_key = event["Records"][0]["s3"]["object"]["key"]

    # Build S3 file path
    s3_input_path = f"s3://{bucket}/{input_key}"

    # Start Glue job
    glue.start_job_run(
        JobName="automated_etl_spport_tickets",
        Arguments={
            "--input_file_path": s3_input_path
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Support ticket ETL job started successfully!")
    }