import json
import os
import psycopg2


# Environment variables (set in Lambda configuration)
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT", "5439")
REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_TABLE = os.getenv("REDSHIFT_TABLE", "public.support_logs")
IAM_ROLE = os.getenv("REDSHIFT_COPY_ROLE")


def lambda_handler(event, context):
    """Load a Parquet file from S3 into Amazon Redshift using the COPY command."""

    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    input_key = record["s3"]["object"]["key"]

    s3_input_path = f"s3://{bucket_name}/{input_key}"
    print(f"Loading data from {s3_input_path}")

    # Connect to Redshift
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DATABASE,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )

    cursor = conn.cursor()

    # Redshift COPY command
    copy_sql = f"""
        COPY {REDSHIFT_TABLE}
        FROM '{s3_input_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET
        REGION 'ap-southeast-2';
    """

    cursor.execute(copy_sql)
    conn.commit()

    print("Data loaded successfully!")

    cursor.close()
    conn.close()

    return {
        "statusCode": 200,
        "body": json.dumps("Redshift load completed successfully.")
    }