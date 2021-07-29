import boto3
import urllib.parse
import pandas as pd
import uuid

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
destination_bucket_name = "crem-coalesce-poc-silver"
cdc_folder = "cdc-files"


def lambda_handler(event, context):
    # ===============
    # Parse response for S3 event and determine object's origin - bucket and key
    # ===============
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"Event is: {event}")
    print(f"Source Bucket is: {bucket} and Key is {key}")

    # ===============
    # Fetch object data into Lambda space
    # ===============
    response = s3_client.get_object(Bucket=bucket, Key=key)
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")

        # Check if from cdc-files
        prefix_list = key.split('/')
        cdc = prefix_list[1]

        if cdc == cdc_folder:
            print("Process CDC")

            output_list = []

            response = s3_client.get_object(Bucket=bucket, Key=key)
            csv_data = response["Body"].read().decode('utf-8')
            rows = csv_data.split("\n")

            for row in rows:
                if row == '':
                    continue
                row = row + f"|{uuid.uuid4()}"
                row = row + '\n'
                output_list.append(row)

            string = ''.join(output_list)
            transfer_files_target_s3(string, key)

        else:
            print("Process Initial Load")
            print("Outputting InitialLoad related dataframe to CSV")

            print("Creating dataframe object's data")
            df = pd.read_csv(response.get("Body"), sep='|')
            # Add unique UID to each row
            df['rowid'] = df.index.to_series().map(lambda x: uuid.uuid4())
            csv_data = df.to_csv(index=False, sep='|')
            transfer_files_target_s3(csv_data, key)
    else:
        print(f"Unsuccessful S3 put_object response. Status - {status}")

    return {
        'statusCode': 200,
        'body': "Successful transfer"
    }


def transfer_files_target_s3(csv_data, key):
    print("Sending object to target bucket")
    response = s3_client.put_object(Bucket=destination_bucket_name, Key=key, Body=csv_data)
    print(f"Destination bucket={destination_bucket_name} | Key={key}")
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    print(f"Successful S3 put_object response. Status - {status}")