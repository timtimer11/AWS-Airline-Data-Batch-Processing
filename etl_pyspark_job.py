import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node dim_airport_code_read
dim_airport_code_read_node1706184191500 = glueContext.create_dynamic_frame.from_catalog(
    database="airlines",
    table_name="dev_airlines_airports_dim",
    redshift_tmp_dir="s3://catalog-temporary", # temporary bucket for redshift
    transformation_ctx="dim_airport_code_read_node1706184191500",
)

# Script generated for node daily_raw_flight_data_from_s3
daily_raw_flight_data_from_s3_node1706183652112 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="airlines",
        table_name="daily_raw",
        transformation_ctx="daily_raw_flight_data_from_s3_node1706183652112",
    )
)

# Script generated for node Join
Join_node1706184940859 = Join.apply(
    frame1=daily_raw_flight_data_from_s3_node1706183652112,
    frame2=dim_airport_code_read_node1706184191500,
    keys1=["originairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1706184940859",
)

# Script generated for node departure_airport_schema_changes
departure_airport_schema_changes_node1706185063523 = ApplyMapping.apply(
    frame=Join_node1706184940859,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("destairportid", "long", "destairportid", "long"),
        ("depdelay", "long", "dep_delay", "bigint"),
        ("arrdelay", "long", "arr_delay", "bigint"),
        ("city", "string", "dep_city", "string"),
        ("name", "string", "dep_airport", "string"),
        ("state", "string", "dep_state", "string"),
    ],
    transformation_ctx="departure_airport_schema_changes_node1706185063523",
)

# Script generated for node Join
Join_node1706185321008 = Join.apply(
    frame1=departure_airport_schema_changes_node1706185063523,
    frame2=dim_airport_code_read_node1706184191500,
    keys1=["destairportid"],
    keys2=["airport_id"],
    transformation_ctx="Join_node1706185321008",
)

# Script generated for node Change Schema
ChangeSchema_node1706185387276 = ApplyMapping.apply(
    frame=Join_node1706185321008,
    mappings=[
        ("carrier", "string", "carrier", "string"),
        ("dep_state", "string", "dep_state", "string"),
        ("state", "string", "arr_state", "string"),
        ("arr_delay", "bigint", "arr_delay", "long"),
        ("city", "string", "arr_city", "string"),
        ("name", "string", "arr_airport", "string"),
        ("dep_city", "string", "dep_city", "string"),
        ("dep_delay", "bigint", "dep_delay", "long"),
        ("dep_airport", "string", "dep_airport", "string"),
    ],
    transformation_ctx="ChangeSchema_node1706185387276",
)

# Script generated for node redshift_fact_table_Write
redshift_fact_table_Write_node1706185473440 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=ChangeSchema_node1706185387276,
        database="airlines",
        table_name="dev_airlines_daily_flights_fact",
        redshift_tmp_dir="s3://catalog-temporary",
        additional_options={"aws_iam_role": "arn:aws:iam::730335519143:role/redshift"},
        transformation_ctx="redshift_fact_table_Write_node1706185473440",
    )
)

job.commit()
