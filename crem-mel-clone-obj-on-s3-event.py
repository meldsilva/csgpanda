import json
import boto3
import urllib.parse
import pandas as pandas
import datetime

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

    response = s3_client.get_object(Bucket={bucket}, Key={key})

    print(response)

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

    # # Function to apply UID to each row in CSV file that landed in the bucket
    # def load_dataframe(sourcefile):
    #     start_time = datetime.datetime.now()
    #     print(f"Started method load_dataframe() / time: {start_time}")
    #     self.df = pd.read_csv(key)
    #     print(f"Loaded DF / time: {datetime.datetime.now()}")

    #     # print(self.df)
    #     self.df['uuid'] = self.df.index.to_series().map(lambda x: uuid.uuid4())
    #     end_time = datetime.datetime.now()
    #     print(f"Added uuid for each row in DF / time: {end_time}")
    #     print(f"Seconds taken to complete uuid updates to DF / time: {abs((end_time - start_time).seconds)}")
    #     # self.df.to_csv("data/pandamonium.csv")
    #     # print(f"Loaded DF / time:{datetime.datetime.now()}")