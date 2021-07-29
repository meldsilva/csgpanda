import json
import boto3
import urllib.parse
import pandas as pd
import datetime
import uuid

s3 = boto3.resource('s3')
destination_bucket_name = "crem-the-bucket-ends-here"


def lambda_handler(event, context):
    s3 = boto3.resource('s3')

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"Event is: {event}")
    print(f"Bucket is: {bucket}")
    print(f"Key is: {key}")

    print(f"Target file is {key}")

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")

        print("Reading from CSV into dataframe")
        df = pd.read_csv(response.get("Body"), sep='|')
        print("Applying rowid")
        df['rowid'] = df.index.to_series().map(lambda x: uuid.uuid4())
        print("Outputting dataframe for CSV object in memory")
        csv_data = df.to_csv(index=False, sep='|')

        print("Sending object to target bucket")
        response = s3_client.put_object(Bucket=destination_bucket_name, Key=key, Body=csv_data)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        print(f"Successful S3 put_object response. Status - {status}")

    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")

    # # create object to copy
    # source = {
    #     'Bucket': bucket,
    #     'Key': key
    # }

    # bucket = s3.Bucket(destination_bucket_name)
    # bucket.copy(source, key)

    return {
        'statusCode': 200,
        'body': key
    }
