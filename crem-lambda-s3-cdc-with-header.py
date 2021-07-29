import boto3
import urllib.parse
import pandas as pd
import uuid

s3 = boto3.resource('s3')
s3_client = boto3.client('s3')
destination_bucket_name = "crem-the-bucket-ends-here"
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
        print("Creating dataframe object's data")
        df = pd.read_csv(response.get("Body"), sep='|')

        # Add unique UID to each row
        df['rowid'] = df.index.to_series().map(lambda x: uuid.uuid4())

        # ===============
        # Determine if data is from full load or change data capture (CDC)
        print(list(df.columns.values.tolist()))
        first_column = list(df.columns.values.tolist())[0]

        if first_column == "Op":
            print("Process CDC")

            prefix_list = key.split('/')
            client_name = prefix_list[0]
            schema_name = prefix_list[1]
            table_name = prefix_list[2]
            object_name = prefix_list[3]

            print(f"Client name: {client_name}")
            print(f"Table name: {table_name}")
            print(f"Schema name: {schema_name}")

            cdc_key = f"{client_name}/{cdc_folder}/{object_name}"
            print(cdc_key)

            df.insert(loc=1, column='table_name', value=table_name)
            df.insert(loc=2, column='schema_name', value=schema_name)
            # print(list(df.columns.values.tolist()))
            print("Outputting CDC related dataframe to CSV")
            csv_data = df.to_csv(index=False, sep='|', header=False)
            transfer_files_target_s3(csv_data, cdc_key)
        else:
            print("Process Initial Load")
            print("Outputting InitialLoad related dataframe to CSV")
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
    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    print(f"Successful S3 put_object response. Status - {status}")