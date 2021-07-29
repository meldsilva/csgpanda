import json
import boto3

s3 = boto3.client('s3')
bucket_list_list = []
bucket = "crem-the-bucket-starts-here"
prefix = "data-source/schema/table/"


def lambda_handler(event, context):
    # Code to determine last created obj
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix="")
    latest = None
    for page in page_iterator:
        if "Contents" in page:
            latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
            if latest is None or latest2['LastModified'] > latest['LastModified']:
                latest = latest2
                print(latest['Key'])
    return {
        'statusCode': 200,
        'body': latest['Key']

    }