import pandas as pd
import uuid
import string
import random
from s3_client import S3_Client
from pandarama import Pandamonium

start_bucket = "crem-the-bucket-starts-here"
end_bucket = "crem-the-bucket-ends-here"
crem_de_la_crem_bucket = "crem-the-la-crem"
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

# 5. Copy file with a new name in same bucket
# alphabet = string.ascii_lowercase
# random_str = ''.join(random.choice(alphabet) for i in range(15))
#
# source_file_name = "testlambda.csv"
# dest_file = source_file_name.split('.')
# dest_file_name = dest_file[0]
# #
# s3.copy_object_in_same_bucket(
#     source_bucket,
#     f"data-source/schema/table/{source_file_name}",
#     f"data-source/schema/table/{dest_file_name}_{random_str}.csv")

# s3.copy_object_in_same_bucket(
#     crem_de_la_crem_bucket,
#     "animals.csv",
#     f"{dest_file_name}_{random_str}.csv")

# 6. List name of last object created
# s3.get_last_created_object(source_bucket)
# s3.get_most_recent_s3_object(start_bucket,"data-source/schema/table/")
# s3.bucket_object_list(crem_de_la_crem_bucket)
input_file = "C:/TestData/CSV-PIPED/CDC_TXN.csv"
pandamonium = Pandamonium(input_file)
pandamonium.load_dataframe()
