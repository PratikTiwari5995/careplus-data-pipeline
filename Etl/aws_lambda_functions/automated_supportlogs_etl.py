import json
import re
import io

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def read_logs_from_s3(bucket: str, key: str) -> str:
    """Read a log file from S3 and return its contents as a UTF-8 string."""
    
    s3 = boto3.client("s3")

    response = s3.get_object(Bucket=bucket, Key=key)
    log_data = response["Body"].read().decode("utf-8")

    return log_data


def parse_log_entries(entries: list) -> list:
    """Parse log entries using regex and return structured log records."""

    pattern = re.compile(
        r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+"
        r"\[(?P<level>[^\]]+)\]\s+"
        r"(?P<service>.*?)\s+-\s+"
        r"TicketID=(?P<TicketID>\S+)\s+"
        r"SessionID=(?P<SessionID>\S+)\s*"
        r"IP=(?P<IP>.*?)\s*\|\s*"
        r"ResponseTime=(?P<ResponseTime>\d+)ms\s*\|\s*"
        r"CPU=(?P<CPU>[\d.]+)%\s*\|\s*"
        r"EventType=(?P<EventType>.*?)\s*\|\s*"
        r"Error=(?P<Error>\w+)\s*"
        r'UserAgent="(?P<UserAgent>.*?)"\s*'
        r'Message="(?P<Message>.*?)"\s*'
        r'Debug="(?P<Debug>.*?)"\s*'
        r"TraceID=(?P<TraceID>.*)",
        re.DOTALL,
    )

    parsed = []
    failed_logs = []

    for i, entry in enumerate(entries):
        match = pattern.search(entry)

        if match:
            parsed.append(match.groupdict())
        else:
            failed_logs.append((i, entry))

    if failed_logs:
        print(f"{len(failed_logs)} logs failed to parse.")

    return parsed


def transform_logs(parsed_logs: list) -> pd.DataFrame:
    """Clean and transform parsed log records into a structured DataFrame."""

    df = pd.DataFrame(parsed_logs)

    if "TraceID" in df.columns:
        df = df.drop("TraceID", axis=1)

    df = df.astype({
        "ResponseTime": "int",
        "CPU": "float"
    })

    df["timestamp"] = pd.to_datetime(
        df["timestamp"],
        format="%Y-%m-%d %H:%M:%S",
        errors="coerce"
    )

    df["Error"] = df["Error"].str.lower().map({
        "true": True,
        "false": False
    })

    fix_level = {
        "INF0": "INFO",
        "DEBG": "DEBUG",
        "warnING": "WARNING"
    }

    df["level"] = df["level"].replace(fix_level)

    df = df.drop_duplicates()

    print("\nData Types:")
    df.info()

    print("\nStatistics:")
    print(df.describe())

    print("\nLog Level Distribution:")
    print(df["level"].value_counts())

    print("\nDuplicate Rows:", df.duplicated().sum())

    return df


def write_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Convert a DataFrame to Parquet format and upload it to S3."""

    s3 = boto3.client("s3")

    table = pa.Table.from_pandas(df, preserve_index=False)

    buffer = io.BytesIO()
    pq.write_table(table, buffer)

    buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )

    print(f"Parquet file written to s3://{bucket}/{key}")


def lambda_handler(event, context):
    """AWS Lambda function that processes S3 log files and stores them as Parquet."""

    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    key = record["s3"]["object"]["key"]

    output_key = key.replace(".log", ".parquet").replace("raw/", "processed/")

    print(f"Triggered by: {bucket_name}/{key}")

    raw_logs = read_logs_from_s3(bucket_name, key)
    print(f"File length: {len(raw_logs)}")

    entries = [entry.strip() for entry in raw_logs.split("---") if entry.strip()]
    print(f"Number of log entries: {len(entries)}")

    parsed_logs = parse_log_entries(entries)

    df = transform_logs(parsed_logs)

    write_parquet_to_s3(
        df=df,
        bucket=bucket_name,
        key=output_key
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Log processing completed successfully.")
    }