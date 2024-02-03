from pyspark.sql import SparkSession
from operator import add


import time, os, json, boto3, sys, string, socket, json
from datetime import datetime

if __name__ == "__main__":
    
    # Initial setup.
    spark = SparkSession\
        .builder\
        .appName("Part2.1CW_f_my_life")\
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
    
    contracts = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv").rdd
    
    transactions = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv").rdd
    

    
    def transaction_map(line):
        return (line[6], int(line[7]))  # to_address, value.
    
    def contract_map(line):
        return (line[0], line[1])
    
    def good_line(line):
        try:
            int(line[7])  # check if value field is a int.
            if line[6] in ["", None]:  # Check if field 6 has a value.
                return False
            return True
        except:
            return False
        
    
    # Oh ok, I wasn't sure by what aggregate means, I'm guessing that's the mapping and reducing of transactions based on key value "to_address" to the specific contracts, so we dont have all the transactions, only the transactions related to the contracts?
    
    
    # Only transaction sample has headers, but all files within the repository has headers. 
    
    # transactions_sample = spark.read.option("header", True).csv("s3a://" + s3_bucket + "/transactions.csv").rdd
    # contracts_sample = spark.read.csv("s3a://" + s3_bucket + "/contracts.csv").rdd
    
    
    
    
    # unfiltered = transactions.count()
    good_transactions = transactions.filter(good_line)
    
    
    
    
    # print(f"Counts: {unfiltered} -> {good_transactions.count()}")  

    # Counts: 369817358 -> 0
    # That's our problem. filter function drives to 0.
    
    
    
    transactions = good_transactions.map(transaction_map).reduceByKey(add)
    # print("Transactions list...")
    # print(transactions.take(5))
    
    # [('0x6baf48e1c966d16559ce2dddb616ffa72004851e', 5000000000000000), 
    #('0x543807d0af2c58b49d7f25659d0472d4d8b8e8da', 20000000000000000000),
    #(None, 0),
    #('0x7cd05927fe0aa3b6552db959e7539ae28dcbbdcf', 100000000000000000000), 
    #('0x543807d0af2c58b49d7f25659d0472d4d8b8e8da', 20000000000000000000), 
    #('0xbf8d8b4ec992203984f7379b001e87e6943dd5e3', 50000000000000000),
    #('0x543807d0af2c58b49d7f25659d0472d4d8b8e8da', 20000000000000000000), 
    #('0x0000000000000000000000000000000000000000', 0), 
    #('0x0000000000000000000000000000000000000000', 0), 
    #('0x0000000000000000000000000000000000000000', 0)
    
    
    
    
    # Get aggregate sum of transaction.  Group by to_address and then aggregrate sum. 
    # transactions = good_transactions.groupBy(lambda x: x[0])
    # Can do sum, but instead we can just map and reduce to get unique addresses and sums of each. 
    # Reducing the number of rows and data being shuffled. 
    # print(transactions.take(10))
    
    
    
    # Data without reduce.
    # [('0x4bd5f0ee173c81d42765154865ee69361b6ad189', ['0x570ce19176bd0002b04a9179309129bbdaf0c4252ffeb76aedbb038cdf662850', '803989500000000000000']),
    # ('0xc9d4035f4a9226d50f79b73aafb5d874a1b6537e', ['0xe17d4d0c4596ea7d5166ad5da600a6fdc49e26e0680135a2f7300eedfd0d8314', '31337']), 
    # ('0xc8ebccc5f5689fa8659d83713341e5ad19349448', ['0x2ec382949ba0b22443aa4cb38267b1fb5e68e188109ac11f7a82f67571a0adf3', '0']),
    # ('0x5df9b87991262f6ba471f09758cde1c0fc1de734', ['0xe891897177614c91284b6929dc2ada5a87705c4729bda2d5eeae47ea2ca9d175', '31337']),
    # ('0xb608771949021d2f2f1c9c5afb980ad8bcda3985', ['0x35d4f3dae18d72a0d4caf02359ca1844687ff879a2655bdec5c33c9b0f65f795', '100000000000000000000'])]
    
    
    # Data with reduce. 
    #[('0x9a755332d874c893111207b0b220ce2615cd036f', 786380645046925000000000), 
    # ('0x4409fe111f7f0d432fe7d8f5595c2e59ded24ca5', 3992838755924174224), 
    # ('0x6f3bb841dabc8dc9e05a8b26e28bf77f5a61751a', 405000000000000000), 
    # ('0xa69f8c35d3af8f25b227c360324d9bc0dfa62663', 7476102820163355606), 
    # ('0x91c5db8433d7d57b48fd411482b1b08291cc28bc', 128185004497358148469), 
    # ('0x577412c8603d0e68cf6f9d99129e4bc16e5e4299', 3202846400000000), 
    # ('0x87fc667efb4e304b3cca7cda8d74dc85b1406103', 1300000000000000), 
    # ('0x6d1477e00d8dadac39b385ae2fbc86748d1cc3d1', 125950000000000000), 
    # ('0x95ca07816ccb2f3d758fb60df960a9988cf470d5', 14000000000000000), 
    # ('0x090b9b5d3b953f106e715e776fc25d02f75ef896', 7598228994643100142)]
    
    
    
    contracts = contracts.map(contract_map)
    
    # print("Contracts list...")
    # print(contracts.take(5))
    
    # ('0x627646279c8826691227f4a40e5a74a775fdd859', [('0x60606040523615610055576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063661172761461014857806382c90ac01461017e578063b76ea962146101b4575b6101465b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163460405180905060006040518083038185876187965a03f1925050501561013d577f23919512b2162ddc59b67a65e3b03c419d4105366f7d4a632f5d3c3bee9b1cff600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a1610143565b60006000fd5b5b565b005b341561015057fe5b61017c600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610225565b005b341561018657fe5b6101b2600480803573ffffffffffffffffffffffffffffffffffffffff169060200190919050506102c8565b005b610223600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190803590602001908201803590602001908080601f0160208091040260200160405190810160405280939291908181526020018383808284378201915050505050509190505061036b565b005b600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156102825760006000fd5b80600160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505b5b50565b600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156103255760006000fd5b80600060006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505b5b50565b600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156103c85760006000fd5b8173ffffffffffffffffffffffffffffffffffffffff1634826040518082805190602001908083836000831461041d575b80518252602083111561041d576020820191506020810190506020830392506103f9565b505050905090810190601f1680156104495780820380516001836020036101000a031916815260200191505b5091505060006040518083038185876187965a03f192505050151561046e5760006000fd5b5b5b50505600a165627a7a7230582015dd8a30edc4d2b88e51c1e252cdc5a08cd0159c2d5d7b063fdaad85d6e813e40029', '0x66117276,0x82c90ac0,0xb76ea962,0xffffffff', 'FALSE', 'FALSE')])
    
    # (address, [contract rest of data.])
    
    
    # contracts = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv").rdd
    
    
    # transactions = transactions.map(transaction_map)
    # [to_address, [hash, value]]
    
    # Contracts doesnt need to be mapped. 
    

    
    
    # Join by one of the attributes. 
    top_set = transactions.join(contracts).take(10)  # This is causing an infinite loop somewhere
    
    # print("Number of joined sets, ", joined_sets.count())  # 0, there is no connections.
    # print(joined_sets.take(5))
    
    # https://qmplus.qmul.ac.uk/mod/forum/discuss.php?d=446395
    # Error obtained by another student, memory increase didn't solve issue.
    
    
    # (address, (contract_attributes, [hash,value]))
    # print("Joined sets list...")
    # print(joined_sets.take(5))
    
    # [('0x4bd5f0ee173c81d42765154865ee69361b6ad189', (['0x570ce19176bd0002b04a9179309129bbdaf0c4252ffeb76aedbb038cdf662850', '803989500000000000000'], [('0x60606040523615610055576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff168063661172761461014857806382c90ac01461017e578063b76ea962146101b4575b6101465b600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163460405180905060006040518083038185876187965a03f1925050501561013d577f23919512b2162ddc59b67a65e3b03c419d4105366f7d4a632f5d3c3bee9b1cff600060009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a1610143565b60006000fd5b5b565b005b341561015057fe5b61017c600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610225565b005b341561018657fe5b6101b2600480803573ffffffffffffffffffffffffffffffffffffffff169060200190919050506102c8565b005b610223600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190803590602001908201803590602001908080601f0160208091040260200160405190810160405280939291908181526020018383808284378201915050505050509190505061036b565b005b600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156102825760006000fd5b80600160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505b5b50565b600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156103255760006000fd5b80600060006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505b5b50565b600160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156103c85760006000fd5b8173ffffffffffffffffffffffffffffffffffffffff1634826040518082805190602001908083836000831461041d575b80518252602083111561041d576020820191506020810190506020830392506103f9565b505050905090810190601f1680156104495780820380516001836020036101000a031916815260200191505b5091505060006040518083038185876187965a03f192505050151561046e5760006000fd5b5b5b50505600a165627a7a7230582015dd8a30edc4d2b88e51c1e252cdc5a08cd0159c2d5d7b063fdaad85d6e813e40029', '0x66117276,0x82c90ac0,0xb76ea962,0xffffffff', 'FALSE', 'FALSE')]))]
    
    # (address, ([transaction row hash, value], [contract row]))
    
    
    
    
    
    # contracts
    # address, bytecode, function_sighashes, is_erc20, is_erc721, block_number
    
    

    # sort by value of contract. 
    # top10 = joined_sets.takeOrdered(10, key=lambda x: x[1][0][1])
    
    my_bucket_resource = boto3.resource('s3',
                 endpoint_url='http://' + s3_endpoint_url,
                 aws_access_key_id=s3_access_key_id,
                 aws_secret_access_key=s3_secret_access_key)
    
    
    date_time = datetime.now().strftime("%d-%m-%Y_%H:%M:%S")
    
    resource_bucket = my_bucket_resource.Object(s3_bucket,'part2-' + date_time + '/top10.txt')
    resource_bucket.put(Body=json.dumps(top_set)) 
    spark.stop()
    
    
    # Problem is, as said by professor,
    # It looks like you are not aggregating the transactions by to_address at all before the join. You need to do this or the dataset will be huge and you will end up with the error you received. 
    
    # Lecturer blanked my question, meaning I should probably try find the answer myself.
    # https://medium.com/build-and-learn/spark-aggregating-your-data-the-fast-way-e37b53314fad
    
    # Shuffling is big thing for performance with spark, and we should ideally do with the minimum amount of data needed. 
    # aggregateByKey(zeroValue, seqFunc, combFunc, numPartitions=None)
    
    # part216-11-2022_15:22:29/
    # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part2-16-11-2022_15:56:14/ cw_output
    
    # part2-16-11-2022_23:00:42/
    # ccc method bucket cp -r bkt://object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc/part2-16-11-2022_23:00:42/ cw_output