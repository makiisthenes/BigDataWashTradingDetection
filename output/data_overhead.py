# Data Overhead Python
# To obtain the remaining 15% of the coursework marks, we need to complete this.
# The blocks table contains a lot of information that may not strictly be necessary for a functioning cryptocurrency e.g. logs_bloom, sha3_uncles, transactions_root, state_root, receipts_root. Analyse how much space would be saved if these columns were removed. Note that all of these values are hex_strings so you can assume that each character after the first two requires four bits (this is not the case in reality but it would be possible to implement it like this) as this will affect your calculations. (15/40).

# blocks - [number, hash, parent_hash, nonce, sha3_uncles, logs_bloom, transactions_root, state_root, receipts_root, miner, difficulty, total_difficulty, size, extra_data, gas_limit, gas_used, timestamp, transaction_count, base_fee_per_gas]

# This is extremely easy, and will showcase this now.
from pyspark.sql import SparkSession
from operator import add
import time, os, json, boto3, sys, string, socket, json
from datetime import datetime

# from graphframes import *

# Initiation of PySpark session.
spark = SparkSession \
    .builder \
    .appName("Coursework Data Overhead") \
    .getOrCreate()

s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
s3_bucket = os.environ['BUCKET_NAME']
hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

my_bucket_resource = boto3.resource('s3',
                                    endpoint_url='http://' + s3_endpoint_url,
                                    aws_access_key_id=s3_access_key_id,
                                    aws_secret_access_key=s3_secret_access_key)

blocks_rdd = spark.read.option("header", True).csv(
    "s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv").rdd


def block_mapper(line):
    # Parse the specific unneccesary categories that the line has and is good for us.
    # logs_bloom, sha3_uncles, transactions_root, state_root, receipts_root
    hex_count = 0  # Assumption that one character is a hex value, 4 bits per characters.
    sha3_uncles = line[4]  # 0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347
    logs_bloom = line[
        5]  # 0x00000000000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000200000000000000000000000000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000000000000000000000000000
    transactions_root = line[6]  # 0xc11ee2eba07de851effa2ea936c6c099563b49b315c91e547ee146b4c2b83169
    state_root = line[7]  # 0xcdee78e20324b59a596db28ffbcbbc3e42585866872964bc980221ff266306dd
    receipts_root = line[8]  # 0x7cd87bbfa3a1ffa53dbb9d2af7a2fdf2546ae9feb7cb003f3de1f44ef4e80b91

    # I would also add extra data, because most likely not needed in the transaction, more like metadata.
    extra_data = line[13]  # 0xd783010301844765746887676f312e342e32856c696e7578

    # Check if the hex value is a valid hex value, if it is, then add it to the hex_count.
    if str(sha3_uncles).startswith("0x"):
        hex_count += len(sha3_uncles) - 2
    if str(logs_bloom).startswith("0x"):
        hex_count += len(logs_bloom) - 2
    if str(transactions_root).startswith("0x"):
        hex_count += len(transactions_root) - 2
    if str(state_root).startswith("0x"):
        hex_count += len(state_root) - 2
    if str(receipts_root).startswith("0x"):
        hex_count += len(receipts_root) - 2
    if str(extra_data).startswith("0x"):
        hex_count += len(extra_data) - 2

    return ("byte_count", hex_count * 4)



result = blocks_rdd.map(block_mapper).reduceByKey(add)

resource_bucket = my_bucket_resource.Object(s3_bucket, 'data_overhead' + datetime.now().strftime("%d-%m-%Y_%H:%M:%S") + '/data_overhead.txt')
resource_bucket.put(Body=json.dumps(result.collect()))  # Write the result to the bucket. Only one tuple, so fine to collect to memory.
spark.stop()
exit()


# ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/data_overhead05-12-2022_23:34:07/ cw_output
#