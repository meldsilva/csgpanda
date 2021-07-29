import json
import boto3

source_bucket_name = "crem-the-bucket-starts-here"
destination_bucket_name = "crem-the-bucket-ends-here"
key = "data-source/schema/table/pandamonium.csv"


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    print(f"Target file is key")

    # create object to copy
    source = {
        'Bucket': source_bucket_name,
        'Key': key
    }
    bucket = s3.Bucket(destination_bucket_name)
    bucket.copy(source, key)

    return {
        'statusCode': 200,
        'body': key
    }
