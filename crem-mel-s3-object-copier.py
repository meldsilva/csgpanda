import boto3
import json

s3 = boto3.resource('s3')

def lambda_handler(event=None, context=None):
    source = {
        'Bucket': 'crem-the-la-crem',
        'Key': "animals.csv"
    }
    bucket = s3.Bucket('crem-the-la-crem')
    bucket.copy(source, "melos14561561mnhd56156.csv")
    return {
        'statusCode': 200,
        'body': []
    }