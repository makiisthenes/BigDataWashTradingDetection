from pyspark.sql import SparkSession
from operator import add


import time, os, json, boto3, sys, string, socket, json
from datetime import datetime

if __name__ == "__main__":
    
    # Initial setup.
    spark = SparkSession\
        .builder\
        .appName("Part1bCW_f_my_life")\
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    
    # transactions = spark.read.option("header", True).csv("s3a://" + s3_bucket + "/transactions.csv").rdd
    
    transactions = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv").rdd
    
    
    def good_line(line):
        try:
            int(line[11]) # transaction epoch
            int(line[7])  # value
            # int(line.split(",")[11])
            return True
        except:
            return False
    
    
    def transaction_map(line):
        transaction_epoch = int(line[11])
        transaction_value = int(line[7])
        gm_object = time.gmtime(int(transaction_epoch))
        trans_date_str = time.strftime("%m-%Y", gm_object)  
        return (trans_date_str, (1, transaction_value))
    
    
    def transaction_reducer(k, v):
        return [k[0]+v[0], k[1]+v[1]]
        # total count, total value. 
        
    
    # clean_transactions = transactions.filter(good_line)
    
    no_trans_monthly = transactions.map(transaction_map)
    # print(no_trans_monthly.take(5))  # analyse whether it added correctly like we wanted. 
    # [('08-2015', (1, 803989500000000000000)), ('08-2015', (1, 31337)), ('08-2015', (1, 0)), ('08-2015', (1, 31337)), ('08-2015', (1, 100000000000000000000))]
    
    no_trans_monthly = no_trans_monthly.reduceByKey(transaction_reducer)
    # print(no_trans_monthly.take(5))  # analyse whether it added correctly like we wanted. 
    # [('08-2015', [9, 1053999504406636803795])]
    
    no_trans_monthly = no_trans_monthly.sortBy(lambda date: datetime.strptime(date[0], '%m-%Y')).map(lambda x: (x[0], int(x[1][1])/ int(x[1][0])))
    # print(no_trans_monthly.take(5))
    #  [('08-2015', 1.1711105604518186e+20)]
    
    
    date_time = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    my_bucket_resource = boto3.resource('s3',
                 endpoint_url='http://' + s3_endpoint_url,
                 aws_access_key_id=s3_access_key_id,
                 aws_secret_access_key=s3_secret_access_key)

    resource_bucket = my_bucket_resource.Object(s3_bucket,'part1b' + date_time + '/avg_monthly_transactions.txt')
    resource_bucket.put(Body=json.dumps(no_trans_monthly.take(100))) 
    spark.stop()
    
    
    
    # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part1b16-11-2022_17:26:11/ cw_output