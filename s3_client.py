import logging
from filecmp import cmp

import boto3
from botocore.exceptions import ClientError

class S3_Client:
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    def upload_file(self, file_name, bucket, object_name=None):

        print(f"Source file is {file_name}")
        print(f"Bucket name  is {bucket}")
        print(f"Target object path is {object_name}")

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    # boto3 client approach
    # s3 = boto3.client("s3")
    # s3_resource = boto3.resource('s3')
    # List all buckets in S3
    # response = s3.list_buckets()
    # for bucket in response['Buckets']:
    #     print(bucket['Name'])
    # List all objects in specified bucket
    """List objects in a bucket
    :param bucket_name: Name of bucket to inspect for obects
    """

    def bucket_object_list(self, bucket_name):
        s3 = boto3.client("s3")
        print(f"These are the objects bucket: {bucket_name}")
        response = s3.list_objects_v2(Bucket=bucket_name)
        # for obj in response['Contents']:
        #     print(obj['Key'])
        for obj in response.get('Contents', []):
            if len(obj['Key']) > 0:
                print(obj['Key'])

    """Create Bucket
    :param bucket_name: Name of new bucket
    """

    def create_bucket(self, bucket_name):
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=bucket_name)

    """
    Copy bucket contents to another
    :param bucket_name_source: Name of source bucket
    :param bucket_name_target: Name of target bucket
    """

    def copy_bucket_contents(self, source_bucket_name, target_bucket_name):
        print(f"Source bucket is {source_bucket_name}")
        print(f"Source bucket is {target_bucket_name}")

        s3 = boto3.client("s3")
        s3_resource = boto3.resource('s3')

        for key in s3.list_objects_v2(Bucket=source_bucket_name)['Contents']:
            files = key['Key']
            copy_source = {'Bucket': source_bucket_name, 'Key': files}
            s3_resource.meta.client.copy(copy_source, target_bucket_name, files)
            print(files)

    def copy_object_in_same_bucket(self, bucket_name, source_object, target_object):
        s3 = boto3.resource('s3')
        source = {
             'Bucket': bucket_name,
             'Key': source_object
        }
        bucket = s3.Bucket(bucket_name)
        bucket.copy(source, target_object)

    # def get_last_created_object(self, bucket_name):
    #     client = boto3.client('s3')
    #     response = client.list_objects(
    #         Bucket=bucket_name,
    #         Prefix='data-source/schema/table',
    #     )
    #     name = response["Contents"][0]["Key"]
    #     print(response)
    #     print(name)

    def get_most_recent_s3_object(self,bucket_name, prefix):
        s3 = boto3.client('s3')
        paginator = s3.get_paginator( "list_objects_v2" )
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        latest = None
        for page in page_iterator:
            if "Contents" in page:
                latest2 = max(page['Contents'], key=lambda x: x['LastModified'])
                if latest is None or latest2['LastModified'] > latest['LastModified']:
                    latest = latest2
                    print(latest['Key'])
        return latest


