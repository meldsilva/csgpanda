import json
import boto3
import urllib.parse
import pandas as pd
import datetime
import uuid
from boto3.s3.transfer import TransferConfig


s3 = boto3.resource('s3')
destination_bucket_name = "crem-the-bucket-ends-here"

# def lambda_handler(event, context):
#     s3 = boto3.resource('s3')
#
#     bucket = event['Records'][0]['s3']['bucket']['name']
#     key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
#     print(f"Event is: {event}")
#     print(f"Bucket is: {bucket}")
#     print(f"Key is: {key}")
#
#     # pandas apply rowID
#     applyuniqueid(key)
#
#
#
#     # copy file to same bucket with alternate key
#
#     # copy file with alternate key to S3 target
#
#     # delete file with alternate key
#
#     return {
#         'statusCode': 200,
#         'body': key
#     }

# Function to apply UID to each row in CSV file that landed in the bucket
def applyuniqueid(sourcefile):
    start_time = datetime.datetime.now()
    print(f"Started method load_dataframe() / time: {start_time}")

    df = pd.read_csv(sourcefile)
    print(f"Loaded DF / time: {datetime.datetime.now()}")

    df['uuid'] = df.index.to_series().map(lambda x: uuid.uuid4())
    end_time = datetime.datetime.now()

    print(f"Added uuid for each row in DF / time: {end_time}")
    print(f"Seconds taken to complete uuid updates to DF / time: {abs((end_time - start_time).seconds)}")
    # self.df.to_csv("data/pandamonium.csv")
    # print(f"Loaded DF / time:{datetime.datetime.now()}")


    # def send_fileto_target_bucket(file):
    #     print(f"Target file is {file}")
    #
    #     config = TransferConfig(multipart_threshold=1024 * 25,
    #                             max_concurrency=10,
    #                             multipart_chunksize=1024 * 25,
    #                             use_threads=True)
    #
    #     s3 = boto3.resource('s3')
    #     s3.Object(destination_bucket_name, key).upload_file(file,
    #                                                      ExtraArgs={'ContentType': 'text/pdf'},
    #                                                      Config=config,
    #                                                      Callback=ProgressPercentage(file_path)
    #                                                      )
    #
    #     #
    #     # # create object to copy
    #     # source = {
    #     #     'Bucket': bucket,
    #     #     'Key': key
    #     # }
    #     # bucket = s3.Bucket(destination_bucket_name)
    #     # bucket.copy()


def main():
    s3 = boto3.resource('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(f"Event is: {event}")
    print(f"Bucket is: {bucket}")
    print(f"Key is: {key}")

    # pandas apply rowID
    applyuniqueid(key)

    # copy file to same bucket with alternate key

    # copy file with alternate key to S3 target

    # delete file with alternate key

    return {
        'statusCode': 200,
        'body': key
    }


if __name__ == "__main__":
    main()