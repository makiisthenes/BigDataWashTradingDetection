from pyspark.sql import SparkSession
from operator import add


import time, os, json, boto3, sys, string, socket, json
from datetime import datetime

if __name__ == "__main__":
    
    # Initial setup.
    spark = SparkSession\
        .builder\
        .appName("Part2CW_f_my_life")\
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    
    
    print("s3_data_repository_bucket: ", s3_data_repository_bucket)
    print("s3_endpoint_url: ", s3_endpoint_url)
    print("s3_access_key_id: ", s3_access_key_id)
    print("s3_secret_access: ", s3_secret_access_key)
    print("s3_bucket: ", s3_bucket)
    
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    
    # Code here for transformation and actions.
    # block_sample = spark.read.csv("s3a://" + s3_bucket + "/blocks.csv").rdd
    
    block = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv").rdd
    
    
    def block_map(line):
        miner = str(line[9])
        size = int(line[12])
        return (miner, size)
    
    
    
    
    block = block.map(block_map).reduceByKey(add)
    top_miners = block.takeOrdered(10, key=lambda x: -x[1])
    
    
    
    
    my_bucket_resource = boto3.resource('s3',
                 endpoint_url='http://' + s3_endpoint_url,
                 aws_access_key_id=s3_access_key_id,
                 aws_secret_access_key=s3_secret_access_key)
    
    
    date_time = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    
    resource_bucket = my_bucket_resource.Object(s3_bucket,'part3-' + date_time + '/top_miners.txt')
    resource_bucket.put(Body=json.dumps(top_miners)) 
    spark.stop()
    
    
    # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part3-16-11-2022_19:49:23/ cw_output
    # part3-16-11-2022_19:49:23/
    # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part3-16-11-2022_19:54:21/ cw_output
    # part3-16-11-2022_19:54:21/
    
    # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part3-25-11-2022_12:24:07/ cw_output