from pyspark.sql import SparkSession
from operator import add


import time, os, json, boto3, sys, string, socket, json
from datetime import datetime

if __name__ == "__main__":
    
    # Initial setup.
    spark = SparkSession\
        .builder\
        .appName("Part1aCW_f_my_life")\
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

	
    
    # Creating rdd. 
    transactions = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv").rdd
    
    # transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
        
    def good_line(line):
        try:
            int(line[11])
            # int(line.split(",")[11])
            return True
        except:
            return False
        
    
	
    def transaction_map(line):
        
        transaction_epoch = line[11]  # epoch string
        # transaction_epoch = line.split(",")[11]
        gm_object = time.gmtime(int(transaction_epoch))
        trans_date_str = time.strftime("%m-%Y", gm_object)  # only care about each month and will make code much easier to use.
        return (trans_date_str, 1)
    
    
    
    clean_transactions = transactions.filter(good_line)
    
    # init_lines = transactions.count() # 369817358
    # clean_lines = clean_transactions.count()  # 369817358
    
    # print("LINE COUNT::")
    # print(init_lines, clean_lines)
    
    no_trans_monthly = clean_transactions.map(transaction_map).reduceByKey(add).sortBy(lambda date: datetime.strptime(date[0], '%m-%Y'))
    date_time = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")

    
    my_bucket_resource = boto3.resource('s3',
                 endpoint_url='http://' + s3_endpoint_url,
                 aws_access_key_id=s3_access_key_id,
                 aws_secret_access_key=s3_secret_access_key)

    resource_bucket = my_bucket_resource.Object(s3_bucket,'parta' + date_time + '/monthly_transactions.txt')
    resource_bucket.put(Body=json.dumps(no_trans_monthly.take(100))) 
    spark.stop()
    
    # coalesce converts all these stupid files into one big file.
    # spark.createDataFrame(transactions.coalesce(1), ["date", "transaction_monthly"]).write.csv("s3a://" + s3_bucket + "/part1a" + date_time)
    
    
    
    
    
    # output.saveAsTextFile("s3a://" + s3_bucket + "/part1a" + date_time)
    
    # spark.stop()
    
    # ccc create spark part1a.py -d -s
    
    
    # aws --endpoint-url https://ceph-object-rgw.comp-teach.qmul.ac.uk s3 ls s3://data-repository-bkt/ECS765/ethereum-parvulus/
    
    # 2022-11-03 09:59:08 7667336004 blocks.csv
# 2022-11-04 09:15:52   44038293 contract_addresses.txt
# 2022-11-04 09:16:14  718886608 contracts.csv
# 2022-11-04 09:16:44 4723643481 logs.csv
# 2022-11-04 09:17:21 7118160067 receipts.csv
# 2022-11-04 11:11:31     860400 scams.json
# 2022-11-03 12:08:21 24777762986 transaction_hashes.txt
# 2022-11-03 10:27:01 154922109826 transactions.csv

 # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part1a15-11-2022_23:19:27/ cw_output
    
# part1a15-11-2022_10:46:58/

    # aws --endpoint-url https://ceph-object-rgw.comp-teach.qmul.ac.uk s3 ls s3://data-repository-bkt/ECS765/ethereum-parvulus/
    
    
# ccc method bucket ls part1a16-11-2022_00:12:39/

 # https://spark-history.comp-teach.qmul.ac.uk/
    
    
#  ccc method cp bkt://<yourbucket> -r destination

# ccc method bucket ls /parta16-11-2022_11:55:08/



# ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc//parta16-11-2022_11:55:08/monthly_transactions.csv cw_output
