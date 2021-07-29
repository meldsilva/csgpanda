import pandas as pd
import uuid
from s3_client import S3_Client
import string
import random

# for bucket in s3.buckets.all():
# #     print(bucket.name)
# bucketstart = s3.Bucket(bucketstartname)
# # List objects present in a bucket
# response = s3.list_objects(Bucket=bucketstartname,
#                            MaxKeys=10,
#                            Preffix="/")


# billionaires = pd.read_csv("data/pandata.csv")
# billionaires
# print (billionaires)
# print ("top 2 rows")
# print(billionaires.head(2))
# print ("bottom 1 rows")
# print(billionaires.tail(1))
#
# #pick certain columns only
# cdc_rowid = billionaires[["CDC","ID","LastName"]]
# print(cdc_rowid)

# print("UUID is: %s",uuid.uuid4())
# print(uuid.uuid4())
#
# print("Rows to insert")
# inserts = billionaires[billionaires["CDC"] == "I" ]
# print(inserts)
#
# print("Rows to update")
# updates = billionaires[billionaires["CDC"] == "U" ]
# print(updates)
#
# print("Rows to delete")
# deletes = billionaires[billionaires["CDC"] == "D" ]
# print(deletes)

#group by
# print(billionaires.groupby(['FirstName']).max())
# print(billionaires.groupby(['LastName']).max())

source_bucket = "crem-the-bucket-starts-here"
target_bucket = "crem-the-bucket-ends-here"
s3 = S3_Client()
# 1. Create Target Bucket
# s3.create_bucket(target_bucket)
# 2. List objects in source bucket
# s3.bucket_object_list(source_bucket)
# s3.bucket_object_list(target_bucket)

# 3. Upload a file to source
# source_file = 'data\pandata.csv'
# source_file_s3 = source_file.split("\\")
# file_upload = f"data-source/schema/table/{source_file_s3[1]}"
# s3.upload_file(source_file, source_bucket, file_upload)

# 4. Copy source bucket to target
# s3.copy_bucket_contents(source_bucket, target_bucket)
#
# 5. Copy file with a new name in same bucket
# letters = string.ascii_lowercase
# random_str = ''.join(random.choice(letters) for i in range(15))
# source_file_name = "pandata.csv"
# dest_file = source_file_name.split('.')
# dest_file_name = dest_file[0]
# s3.copy_object_in_same_bucket(
#     source_bucket,
#     f"data-source/schema/table/{source_file_name}",
#     f"data-source/schema/table/{dest_file_name}_{random_str}.csv")

# 6. List name of last object created
s3.get_last_created_object(source_bucket)
# s3.bucket_object_list(source_bucket)