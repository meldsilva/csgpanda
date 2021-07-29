BUCKET_NAME = "crem-marky-spark"
PREFIX = "tblCompany/"

datasource0 = glueContext.create_dynamic_frame.from_catalog(database="default", table_name="table_x")
dataF = datasource0.toDF().coalesce(1)

from awsglue.dynamicframe import DynamicFrame
DyF = DynamicFrame.fromDF(dataF, glueContext, "DyF")

datasink2 = glueContext.write_dynamic_frame.from_options(frame = DyF, connection_type = "s3", connection_options = {"path": "s3://" + BUCKET_NAME + "/" + PREFIX}, format = "json", transformation_ctx = "datasink2")

import boto3
client = boto3.client('s3')

response = client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)
name = response["Contents"][0]["Key"]

client.copy_object(Bucket=BUCKET_NAME, CopySource=BUCKET_NAME+name, Key=PREFIX+"new_name")
client.delete_object(Bucket=BUCKET Key=name)