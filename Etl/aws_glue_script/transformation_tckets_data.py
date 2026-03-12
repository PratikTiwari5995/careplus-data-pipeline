import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.gluetypes import *
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame


"""
AWS Glue ETL Job

Reads support ticket CSV files from S3, applies schema and cleaning
transformations, performs data quality checks, and writes the results
to S3 in Parquet format.
"""



# Initialize Glue Job

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_file_path"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)



# Data Quality Rules

DEFAULT_DATA_QUALITY_RULESET = """
Rules = [
    ColumnCount > 0
]
"""



# Read CSV from S3

AmazonS3_node = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ","
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": [args["input_file_path"]],
        "recurse": True
    },
    transformation_ctx="AmazonS3_node"
)



# Apply Schema Mapping

ChangeSchema_node = ApplyMapping.apply(
    frame=AmazonS3_node,
    mappings=[
        ("ticket_id", "string", "ticket_id", "string"),
        ("created_at", "string", "created_at", "timestamp"),
        ("resolved_at", "string", "resolved_at", "timestamp"),
        ("agent", "string", "agent", "string"),
        ("priority", "string", "priority", "string"),
        ("num_interactions", "string", "num_interactions", "int"),
        ("issuecat", "string", "issuecat", "string"),
        ("channel", "string", "channel", "string"),
        ("status", "string", "status", "string"),
        ("agent_feedback", "string", "agent_feedback", "string")
    ],
    transformation_ctx="ChangeSchema_node"
)



# Rename Column

RenameField_node = RenameField.apply(
    frame=ChangeSchema_node,
    old_name="issuecat",
    new_name="issue_category",
    transformation_ctx="RenameField_node"
)



# Filter Invalid Records

Filter_node = Filter.apply(
    frame=RenameField_node,
    f=lambda row: (row["num_interactions"] >= 0),
    transformation_ctx="Filter_node"
)



# Convert to Spark DataFrame

df = Filter_node.toDF()
df.createOrReplaceTempView("myDataSource")


# SQL Transformation
# Fix priority spelling issues

sql_query = """
SELECT
    ticket_id,
    created_at,
    resolved_at,
    agent,
    CASE
        WHEN priority = 'Lw' THEN 'Low'
        WHEN priority = 'Medum' THEN 'Medium'
        WHEN priority IN ('HgH','Hgh') THEN 'High'
        ELSE priority
    END AS priority,
    num_interactions,
    issue_category,
    channel,
    status
FROM myDataSource
"""

result_df = spark.sql(sql_query)



# Convert Back to DynamicFrame

result_dynamic_frame = DynamicFrame.fromDF(
    result_df,
    glueContext,
    "result_dynamic_frame"
)



# Data Quality Check

EvaluateDataQuality().process_rows(
    frame=result_dynamic_frame,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "DQ_check",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)



# Reduce to Single Output File

if result_dynamic_frame.count() >= 1:
    result_dynamic_frame = result_dynamic_frame.coalesce(1)



# Write Parquet to S3

glueContext.write_dynamic_frame.from_options(
    frame=result_dynamic_frame,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://YOUR_BUCKET_NAME/support-tickets/processed/",
        "partitionKeys": []
    },
    format_options={
        "compression": "snappy"
    },
    transformation_ctx="S3_output"
)


job.commit()