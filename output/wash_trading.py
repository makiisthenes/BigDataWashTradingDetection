# Michael 19/11/22
# import igraph as ig  # unknown import for now.

# Wash trading based on Paper 1,2.
# Detecting based on closed loop, DAG.
# Vertices is seen as the addresses of the trade, edges as the transactions between them.

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from operator import add
import time, os, json, boto3, sys, json
from datetime import datetime
# from graphframes import *



# Initiation of PySpark session.
sparkconfig = SparkConf()
sparkconfig.set("spark.ui.port", "35040")


spark = SparkSession\
        .builder\
        .appName("Coursework")\
        .config(conf=sparkconfig)\
        .getOrCreate()



s3_data_repository_bucket = "data-repository-bkt"
s3_endpoint_url = "rook-ceph-rgw-ceph-storage-object.rook-ceph-cluster.svc:8080"
s3_access_key_id = "TLCHLURPRGLT2MCLPTZB"  # os.environ['AWS_ACCESS_KEY_ID']
s3_secret_access_key = "UYCTXqJ1fpFgfvAA2ycvtZ10Qs8q0Hzyrm42iOVB"  # os.environ['AWS_SECRET_ACCESS_KEY']
s3_bucket = "object-bucket-ec20433-62ad507a-9907-4f6a-8055-7daeae09ffcc"  # os.environ['BUCKET_NAME']


"""
hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
hadoopConf.set("fs.s3a.path.style.access", "true")
hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

"""

my_bucket_resource = boto3.resource('s3',
                 endpoint_url='http://' + s3_endpoint_url,
                 aws_access_key_id=s3_access_key_id,
                 aws_secret_access_key=s3_secret_access_key)

# END OF CONFIGURATION.

# SCC component: https://www.youtube.com/watch?v=wUgWX0nc4NY

# They check which trades are sent to specific contracts.



# Directed multi-graph ?
# transactions_rdd = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv").rdd
transactions_rdd = spark.read.option("header", True).csv("s3a://" + s3_bucket + "/washed_transactions.csv").rdd  # sample

# Graph Clustering, Nearest Neighbour Algorithms.
transactions = [
    # Transaction Type:
    #  hash, nonce, block_hash, block_number, transaction_index, from_address, to_address, value, gas, gas_price, input,
    #  block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type.

    # Contract Type:
    # address, bytecode, function_sighashes, is_erc20, is_erc721, block_number.
    ["0x570ce19176bd0002b04a9179309129bbdaf0c4252ffeb76aedbb038cdf662850","0","0xe6fb31b12d06b5a70f420a28b4e034bcd152abc2d6031249f35876f80bf83e8b","46205","0","0x3f2f381491797cc5c0d48296c14fd0cd00cdfa2d","0x4bd5f0ee173c81d42765154865ee69361b6ad189","803989500000000000000","21000","500000000000","0x","1438919175","","","0"],
    # Convert line to list above.
    # 0xe17d4d0c4596ea7d5166ad5da600a6fdc49e26e0680135a2f7300eedfd0d8314,1,0x6235be352481368721cd978cee3c1e05ce31419b0e353ac7d67eb35c549b6a4e,46214,0,0xa1e4380a3b1f749673e270229993ee55f35663b4,0xc9d4035f4a9226d50f79b73aafb5d874a1b6537e,31337,21750,50000000000000,0x74796d3474406469676978,1438919394,,,0
    ["0xe17d4d0c4596ea7d5166ad5da600a6fdc49e26e0680135a2f7300eedfd0d8314","1","0x6235be352481368721cd978cee3c1e05ce31419b0e353ac7d67eb35c549b6a4e","46214","0","0xa1e4380a3b1f749673e270229993ee55f35663b4","0xc9d4035f4a9226d50f79b73aafb5d874a1b6537e","31337","21750","50000000000000","0x74796d3474406469676978","1438919394","","","0"],

    # The same for the rest of the transactions.
    ["0x2ec382949ba0b22443aa4cb38267b1fb5e68e188109ac11f7a82f67571a0adf3","0","0xeab8fe9da0b41b2c003db620bb9adbedd5fcc7222cc50d53431aa1df47a20dda","46217","0","0xc8ebccc5f5689fa8659d83713341e5ad19349448","0xc8ebccc5f5689fa8659d83713341e5ad19349448","0","21000","65334370444","0x","1438919451","","","0"],
    ["0xe891897177614c91284b6929dc2ada5a87705c4729bda2d5eeae47ea2ca9d175","2","0xf5674f4a8c62bb27480155adfbaf272b8bc969509f5a9ce00600be802d00e1f2","46219","0","0x4bd5f0ee173c81d42765154865ee69361b6ad189","0xfd2605a2bf58fdbb90db1da55df61628b47f9e8c","31337","21800","50000000000000","0x74796d3474406469676978","1438919461","","","0"],
    ["0x35d4f3dae18d72a0d4caf02359ca1844687ff879a2655bdec5c33c9b0f65f795","0","0x1e6f0d21ce2260371c95266b5cf4f698841c8c57b4a1dfb4feee2fd0cb3f2021","46220","0","0xf0cf0af5bd7d8a3a1cad12a30b097265d49f255d","0xb608771949021d2f2f1c9c5afb980ad8bcda3985","100000000000000000000","21000","64178193561","0x","1438919491","","","0"],
    ["0x41738785c4330ce9531aed26b21b9cfba6f27b9183d11355f4d9522211c2a44e","0","0xc3389d535ebbaae818c492901ca40995a2c9b644af71f8d57d7a93e6a0871bcf","46230","0","0x1c68a66138783a63c98cc675a9ec77af4598d35e","0xc8ebccc5f5689fa8659d83713341e5ad19349448","50000000000000000000","21000","71288549894","0x","1438919571","","","0"],
    ["0x80f31704782a53514ab0693499f78922169885a16fd4821d42a4a820d2452799","0","0x7ea3beb17acb66f15fa33ad71f41d1e2a5dd878fc489555e0f57267c4d2a7a03","46235","0","0xfd2605a2bf58fdbb90db1da55df61628b47f9e8c","0x3f2f381491797cc5c0d48296c14fd0cd00cdfa2d","10000000000000000","21000","70563255618","0x","1438919655","","","0"],
    ["0x3a1be2710cc4c46adaf85bdfd42fa80b0f2044b3d976ee57de845981ae5a2430","0","0xd6a6763acc2c642b5ba16904a168efc571a48a8e6d2075eacd44de06fb839d9c","46237","0","0xbbed46565f5aa9af9539f543067821fa4b565438","0xbf8d8b4ec992203984f7379b001e87e6943dd5e3","4406636741121","21000","70543826520","0x","1438919696","","","0"],
    ["0xc0c1c720bc5b3583ad3a4075730b44c0c120a0fe660e51817f8c857bf37dbec0","0","0xbee4330cdd56d2bcc47fa42e52e3c089c649b209acc0ae5611f6c7bc7c0b350e","46239","0","0x8ce4949d8a16542d423c17984e6739fa72ceb177","0x15e34a8324164ef0890471f6f527451f7a22cf12","100000000000000000000","21000","1000000000000","0x","1438919756","","","0"],

]




# ██████╗  █████╗ ██████╗ ████████╗     ██╗       ███████╗███████╗██╗     ███████╗    ████████╗██████╗  █████╗ ██████╗ ██╗███╗   ██╗ ██████╗     ███████╗██╗██╗  ████████╗███████╗██████╗ ██╗███╗   ██╗ ██████╗
# ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝    ███║██╗    ██╔════╝██╔════╝██║     ██╔════╝    ╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██║████╗  ██║██╔════╝     ██╔════╝██║██║  ╚══██╔══╝██╔════╝██╔══██╗██║████╗  ██║██╔════╝
# ██████╔╝███████║██████╔╝   ██║       ╚██║╚═╝    ███████╗█████╗  ██║     █████╗         ██║   ██████╔╝███████║██║  ██║██║██╔██╗ ██║██║  ███╗    █████╗  ██║██║     ██║   █████╗  ██████╔╝██║██╔██╗ ██║██║  ███╗
# ██╔═══╝ ██╔══██║██╔══██╗   ██║        ██║██╗    ╚════██║██╔══╝  ██║     ██╔══╝         ██║   ██╔══██╗██╔══██║██║  ██║██║██║╚██╗██║██║   ██║    ██╔══╝  ██║██║     ██║   ██╔══╝  ██╔══██╗██║██║╚██╗██║██║   ██║
# ██║     ██║  ██║██║  ██║   ██║        ██║╚═╝    ███████║███████╗███████╗██║            ██║   ██║  ██║██║  ██║██████╔╝██║██║ ╚████║╚██████╔╝    ██║     ██║███████╗██║   ███████╗██║  ██║██║██║ ╚████║╚██████╔╝
# ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝        ╚═╝       ╚══════╝╚══════╝╚══════╝╚═╝            ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚═╝╚═╝  ╚═══╝ ╚═════╝     ╚═╝     ╚═╝╚══════╝╚═╝   ╚══════╝╚═╝  ╚═╝╚═╝╚═╝  ╚═══╝ ╚═════╝
#
# In essence, wash trading can be seen as individuals that are trading with themselves, so we can filter traders by that, this is easy to do with MapReduce paradigm.


# Filter self trades (same addresses),
def filter_self_map(line):
    from_address = line[5]
    to_address   = line[6]
    trans_value  = line[7]
    timestamp    = line[11]
    transaction_index = line[4]
    if from_address == to_address:
        return (from_address, [transaction_index, trans_value, timestamp])
    else:
        return ("none", 1)

def filter_self_reduce(line1, line2):
    if line1[0] != "none":
        return (line1[0], [line1[1]+line2[1], line1[1].append(line2[1])])
    else:
        return ("none", line1[1]+line2[1])

def filter_self_trades(rdd):
    filtered_rdd = rdd.map(filter_self_map).reduceByKey(filter_self_reduce).filter(lambda x: x[0] != "none")
    return filtered_rdd

# Filter self trades.
filtered_self_transactions = filter_self_trades(transactions_rdd)

# Save these filtered self transactions to S3.
my_bucket_resource = my_bucket_resource.Object(s3_bucket,'cw-' + datetime.now().strftime("%d-%m-%Y_%H:%M:%S") + '/potential_self_wash_traders.txt')
my_bucket_resource.put(Body=json.dumps(filtered_self_transactions.take(10)))
# self_traders = filtered_self_transactions.count()

# Filter out self transactions, by antijoin from self .
hashes_ids = filtered_self_transactions.map(lambda line: ("hashes", [*line[1][1]])).reduceByKey(lambda x, y: x[0]+y[0]).collect()[0][1]
filtered_transactions = transactions_rdd.map(lambda line: line if line[0] not in hashes_ids else ("none", None)).filter(lambda x: x[0] != "none")
# filtered_transactions = transactions_rdd.join(filtered_self_transactions, transactions[0] != filtered_self_transactions[2], "leftanti")  # Leftanti is the same as anti join, using transaction_index as condition key.
# Works with sample, shows the self traders. Solved first part.




# ██████╗  █████╗ ██████╗ ████████╗    ██████╗        ██████╗  ██████╗ ███████╗███████╗██╗██████╗ ██╗     ███████╗     ██████╗ █████╗ ███╗   ██╗██████╗ ██╗██████╗  █████╗ ████████╗███████╗███████╗
# ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝    ╚════██╗██╗    ██╔══██╗██╔═══██╗██╔════╝██╔════╝██║██╔══██╗██║     ██╔════╝    ██╔════╝██╔══██╗████╗  ██║██╔══██╗██║██╔══██╗██╔══██╗╚══██╔══╝██╔════╝██╔════╝
# ██████╔╝███████║██████╔╝   ██║        █████╔╝╚═╝    ██████╔╝██║   ██║███████╗███████╗██║██████╔╝██║     █████╗      ██║     ███████║██╔██╗ ██║██║  ██║██║██║  ██║███████║   ██║   █████╗  ███████╗
# ██╔═══╝ ██╔══██║██╔══██╗   ██║       ██╔═══╝ ██╗    ██╔═══╝ ██║   ██║╚════██║╚════██║██║██╔══██╗██║     ██╔══╝      ██║     ██╔══██║██║╚██╗██║██║  ██║██║██║  ██║██╔══██║   ██║   ██╔══╝  ╚════██║
# ██║     ██║  ██║██║  ██║   ██║       ███████╗╚═╝    ██║     ╚██████╔╝███████║███████║██║██████╔╝███████╗███████╗    ╚██████╗██║  ██║██║ ╚████║██████╔╝██║██████╔╝██║  ██║   ██║   ███████╗███████║
# ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝       ╚══════╝       ╚═╝      ╚═════╝ ╚══════╝╚══════╝╚═╝╚═════╝ ╚══════╝╚══════╝     ╚═════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚═════╝ ╚═╝╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚══════╝
#
# Now that we have removed the self traders, based on lecturer advice we can now look at potential candidates for wash trading.
# We first want to map, each address, with net value for the transaction.



def part2_map(line):
    # not sure of filtered_transaction is like that now.
    from_addr = line[5]
    to_addr   = line[6]
    value     = line[7]
    return ([from_addr, -int(value)], [to_addr, int(value)])


filtered_count = filtered_transactions.count()
avg_trans_value = filtered_transactions.map(lambda line: int(line[7])).reduce(add) / filtered_count # need to increase the fields send from original transactions.

net_transactions = filtered_transactions.flatMap(part2_map).reduceByKey(add)
# [('0x3f2f381491797cc5c0d48296c14fd0cd00cdfa2d', -803979500000000000000), ('0x4bd5f0ee173c81d42765154865ee69361b6ad189', 803989499999999968663), ('0xa1e4380a3b1f749673e270229993ee55f35663b4', -31337)]



# Now we will have a rdd of net_transactions, we only want the candidates that have a net value of 0, or really close to 0, as possible candidates for wash trading.
# 0 or 15% of average sum of transactions.
# if net_value < 0.15 * avg_trans_value:
#    print("Possible wash trading candidate: ", net_value)
# Do this using map and reduce, code is shown below:






possible_candidates = net_transactions.filter(lambda x: abs(x[1]) < 0.15 * avg_trans_value)

print(f"Possible candidates for wash trading: {possible_candidates.count()} out of {filtered_count}.")
print(possible_candidates.take(5))
# [('0xa1e4380a3b1f749673e270229993ee55f35663b4', -31337), ('0xc9d4035f4a9226d50f79b73aafb5d874a1b6537e', 31337), ('0xfd2605a2bf58fdbb90db1da55df61628b47f9e8c', -9999999999968663), ('0xbbed46565f5aa9af9539f543067821fa4b565438', -4406636741121), ('0xbf8d8b4ec992203984f7379b001e87e6943dd5e3', 4406636741121)]


# Big values anomalies, make more transactions seem like candidates for wash trading.



# ██████╗  █████╗ ██████╗ ████████╗    ██████╗         ██████╗██████╗ ███████╗ █████╗ ████████╗██╗███╗   ██╗ ██████╗      ██████╗ ██████╗  █████╗ ██████╗ ██╗  ██╗
# ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝    ╚════██╗██╗    ██╔════╝██╔══██╗██╔════╝██╔══██╗╚══██╔══╝██║████╗  ██║██╔════╝     ██╔════╝ ██╔══██╗██╔══██╗██╔══██╗██║  ██║
# ██████╔╝███████║██████╔╝   ██║        █████╔╝╚═╝    ██║     ██████╔╝█████╗  ███████║   ██║   ██║██╔██╗ ██║██║  ███╗    ██║  ███╗██████╔╝███████║██████╔╝███████║
# ██╔═══╝ ██╔══██║██╔══██╗   ██║        ╚═══██╗██╗    ██║     ██╔══██╗██╔══╝  ██╔══██║   ██║   ██║██║╚██╗██║██║   ██║    ██║   ██║██╔══██╗██╔══██║██╔═══╝ ██╔══██║
# ██║     ██║  ██║██║  ██║   ██║       ██████╔╝╚═╝    ╚██████╗██║  ██║███████╗██║  ██║   ██║   ██║██║ ╚████║╚██████╔╝    ╚██████╔╝██║  ██║██║  ██║██║     ██║  ██║
# ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝       ╚═════╝         ╚═════╝╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝╚═╝  ╚═══╝ ╚═════╝      ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝
#

# Now we want to make edges and vertices for the graph.
# We will try implement a new mapper function to do this.
# We can then use the reduceByKey function to aggregate the values for each key.
# Instead we can make a dataframe for edges, and vertices and then create a graph from that. [We initially tried to do with class based solution.]
# https://stackoverflow.com/questions/45720931/pyspark-how-to-visualize-a-graphframe

# We need Graphframes, which provide Dataframe based graph processing, but it isn't available in the cluster currently connected to, we will still try to code the necessary functions to make it work.
# https://graphframes.github.io/graphframes/docs/_site/index.html
# More specifically we need:
# https://graphframes.github.io/graphframes/docs/_site/user-guide.html#strongly-connected-components
# https://graphframes.github.io/graphframes/docs/_site/api/python/graphframes.html#graphframes.GraphFrame.stronglyConnectedComponents

from graphframes import GraphFrame

# 1st step is to create edges and vertices for this graph.
# Vertices
def vertices_map(line):  # We need to flat map this.
    from_address = line[1]
    to_address   = line[2]
    token_type   = line[11]
    return ([from_address, token_type], [to_address, token_type])   # [transaction_address, token_type]

# Edges
def edge_map(line):
    # Indexes will vary based on coursework dataset.
    from_address = line[4]
    to_address   = line[5]
    value        = line[6]
    return ((from_address, to_address), (1, value))  # Create edge key and value pairs. [seller_address, buyer_address, value,  weight, connection_type]

# Obtain the vertices and edges RDDs.
transaction_vertices = filtered_transactions.flatMap(vertices_map).reduceByKey(lambda x, y: x)  # Reduce will leave distinct value for each key. Does the code reflect this? Answer: Yes.
transaction_edges = filtered_transactions.map(edge_map).reduceByKey(lambda x, y: (x[1][0]+y[1][0], x[1][1]+y[1][1]))  # ((src,dst), (weight, value))

# Convert to Dataframes.
transaction_vertices_dataframe = spark.createDataFrame(transaction_vertices, ["address", "token_type"])  # added weight to vertices so we can filter then if no edges exist.
transaction_edges_dataframe = spark.createDataFrame(transaction_edges, ["src", "dst", "weight", "value"])


# Start to make the graph.
# graph = GraphFrame(transaction_vertices_dataframe, transaction_edges_dataframe)

# Now we have a graph based dataframe, we can now work on calculating the strongly connected components.



# ██████╗  █████╗ ██████╗ ████████╗    ██╗  ██╗       ███████╗ ██████╗ ██████╗     ██████╗ ███████╗███╗   ██╗███████╗██████╗  █████╗ ████████╗██╗ ██████╗ ███╗   ██╗
# ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝    ██║  ██║██╗    ██╔════╝██╔════╝██╔════╝    ██╔════╝ ██╔════╝████╗  ██║██╔════╝██╔══██╗██╔══██╗╚══██╔══╝██║██╔═══██╗████╗  ██║
# ██████╔╝███████║██████╔╝   ██║       ███████║╚═╝    ███████╗██║     ██║         ██║  ███╗█████╗  ██╔██╗ ██║█████╗  ██████╔╝███████║   ██║   ██║██║   ██║██╔██╗ ██║
# ██╔═══╝ ██╔══██║██╔══██╗   ██║       ╚════██║██╗    ╚════██║██║     ██║         ██║   ██║██╔══╝  ██║╚██╗██║██╔══╝  ██╔══██╗██╔══██║   ██║   ██║██║   ██║██║╚██╗██║
# ██║     ██║  ██║██║  ██║   ██║            ██║╚═╝    ███████║╚██████╗╚██████╗    ╚██████╔╝███████╗██║ ╚████║███████╗██║  ██║██║  ██║   ██║   ██║╚██████╔╝██║ ╚████║
# ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝            ╚═╝       ╚══════╝ ╚═════╝ ╚═════╝     ╚═════╝ ╚══════╝╚═╝  ╚═══╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝
#
# Candidate Set Generation with SCCs
# Account Candidate Set Generation

# sccs = graph.stronglyConnectedComponents(maxIter=10)  # not sure about what max iter is, would need to look at algorithm for Strongly connected components.
# sccs contain a DataFrame with new vertex column “component”, which provides the component ID of each vertex if they are part of a given scc.
# sccs_rdd = sccs.rdd
# Example view of sccs_rdd, educated guess.
# [address, token_type, component]

# Do some calculation on set of vertices within a specific set denoted by the same component id.
# The paper does a hash to determine the different sets, a list of vertices have.


# scc_vertexes = sccs_rdd.map(lambda x: (x[2], [x[0], 0])).reduceByKey(lambda x, y: x[1][0] + y[1][0])  # [component, [address1, address2, ...]]

# sccs_rdd.map # component_id, address.

# map it and count how many sccs each vertex is part of

# Need to see how this looks like.
# We iterate through this scc, decrementing weight of edges. Until there are no more edges left. At the same time,
# increment of scc weight in order to get candidate set based on threshold.

def scc_vertex_algo(line):
    component_id = line[0]
    addresses = line[1]
    scc_count = line[2]+1  # increase scc count by 1, but this isn't stored persistently for us to use.
    return (component_id, [addresses, scc_count])

def edges_algo_map(line):
    src = line[0][0]
    dst = line[0][1]
    weight = line[1] - 1
    return ([src, dst], weight)



# We might need to join edges and vertices together.

edges = transaction_edges  # ((src, dest), weight)
vertices = transaction_vertices  # ((src,dst), (weight, value))

while edges.count() > 0:
    # Start to make the graph.

    # Now we have a graph based dataframe, we can now work on calculating the strongly connected components.
    graph = GraphFrame(transaction_vertices_dataframe, spark.createDataFrame(edges, [("src", "dst"), ("weight", "value")]))

    sccs = graph.stronglyConnectedComponents(maxIter=10)


    scc_rdd_vertex = sccs.vertices.rdd  # [address, token_type, weight, component]
    # Need to test if this is the dataframe/ rdd layout.
    scc_rdd_edges = sccs.edges.rdd  # ["src", "dst", "weight", "value"]


    # Parallelize of hash mapped, sccs_rdd component id by instance.

    # Determine the number of vertexes in each scc.
    scc_vertexes = scc_rdd_vertex.map(lambda x: (x[3], [[x[0]], 0])).reduceByKey(
        lambda x, y: x[0][0] + y[0][0])  # [component, [address1, address2, ...]]


    scc_vertexes = scc_vertexes.map(scc_vertex_algo)  # [component, [address1, address2, ...], weight]

    # Edges has been updated
    edges = edges.rdd.map(edges_algo_map).filter(lambda x: x[1] > 0)

    # Vertices need to be updated based on edges. We don't really care about the vertices, because the scc algorithm will only detect it based on the edges alone.
    # if this doesn't work, we need gf.filterVertices() and remove vertices that automatically remove edges.


    # We need a rdd to store the intermediate results of scc mapping. Component id can change on each iteration, so we need to make the sorted address list the key.
    scc_map_inter = scc_vertexes.map(lambda x: (sorted(x[1][0]), x[1][1]))  # [[addresses], 1]
    if not scc_mapping:
        # [[address1, address2, ...], weight]
        scc_mapping = scc_map_inter
    else:
        scc_mapping = scc_mapping.union(scc_map_inter)  # We will just keep adding scc_vertex counts.

    # parallelize the edges and vertices.




# At the end of this while loop we need to have a mapping of [[address1, address2, ...], weight] in order to get the candidate set.
scc_count = scc_mapping.map(lambda line: line).reduceByKey(add)  # [[addr], weight] simply add the weights together.



# Threshold value of scc.
# If scc weight is greater than threshold, then we add to the candidate set.
THRESHOLD_SCC_COUNT = 3
candidate_set = scc_count.filter(lambda x: x[1] >= THRESHOLD_SCC_COUNT)  # [[address1, address2, ...], weight]



# Given the candidate set, we need to check the SCCs, and sum the scc values individually to see if we get a net neutral position, and evidence of wash trading.


# print(candidate_set.take(1))
# spark.close()
# exit()


# ██████╗  █████╗ ██████╗ ████████╗    ███████╗       ████████╗██████╗  █████╗ ██████╗ ███████╗    ██╗   ██╗ ██████╗ ██╗     ██╗   ██╗███╗   ███╗███████╗
# ██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝    ██╔════╝██╗    ╚══██╔══╝██╔══██╗██╔══██╗██╔══██╗██╔════╝    ██║   ██║██╔═══██╗██║     ██║   ██║████╗ ████║██╔════╝
# ██████╔╝███████║██████╔╝   ██║       ███████╗╚═╝       ██║   ██████╔╝███████║██║  ██║█████╗      ██║   ██║██║   ██║██║     ██║   ██║██╔████╔██║█████╗
# ██╔═══╝ ██╔══██║██╔══██╗   ██║       ╚════██║██╗       ██║   ██╔══██╗██╔══██║██║  ██║██╔══╝      ╚██╗ ██╔╝██║   ██║██║     ██║   ██║██║╚██╔╝██║██╔══╝
# ██║     ██║  ██║██║  ██║   ██║       ███████║╚═╝       ██║   ██║  ██║██║  ██║██████╔╝███████╗     ╚████╔╝ ╚██████╔╝███████╗╚██████╔╝██║ ╚═╝ ██║███████╗
# ╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝       ╚══════╝          ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═════╝ ╚══════╝      ╚═══╝   ╚═════╝ ╚══════╝ ╚═════╝ ╚═╝     ╚═╝╚══════╝
#
# ███╗   ███╗ █████╗ ████████╗██╗  ██╗ ██████╗██╗███╗   ██╗ ██████╗
# ████╗ ████║██╔══██╗╚══██╔══╝██║  ██║██╔════╝██║████╗  ██║██╔════╝
# ██╔████╔██║███████║   ██║   ███████║██║     ██║██╔██╗ ██║██║  ███╗
# ██║╚██╔╝██║██╔══██║   ██║   ██╔══██║██║     ██║██║╚██╗██║██║   ██║
# ██║ ╚═╝ ██║██║  ██║   ██║   ██║  ██║╚██████╗██║██║ ╚████║╚██████╔╝
# ╚═╝     ╚═╝╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝ ╚═════╝╚═╝╚═╝  ╚═══╝ ╚═════╝
#

# Using the set of address we have now obtained, we will need to join to net_transactions we made earlier.


# [edges] = [2,4][] [][]

# sccs_layout = [[address1, address2, ...], weight, [edges]]
# volume_threshold = 0.01
# for scc in sccs:
    #  net_transactions = edges.join(transactions, scc_addresses, (src, address, "").map(net_trans_map).reduce(count_trans_reduce)
    #  sum_transactions = net_transactions.map(lambda line: ("scc_sum", transaction_sum)).reduceByKey(add)   # 60+50+-110 = 0
    # transaction_count = net_transactions.count()  # 3
    # avg_transaction = sum_transactions[0][1] / transaction_count  #  take scc_sum and divide by transaction count
    # threshold = net_transactions.map(lambda line: ("scc_sum", abs(total_scc_value))).reduceByKey(add) * volume_threshold
    # if abs(sum_transactions) <= threshold:
        # print("Wash trading detected")
        # collect in global rdd., if this exists
        # suspects_rdd = suspects_rdd.union(scc)








#[2,3,value] <-- join
# net_trans_map map
# [2, -value]
# [3, value]
# reduce
# [2, net_value]
# [3, net_value]

# candidate_set map
# sum (addresses)




# Get values from the candidate set.
# candidate_set_values = candidate_set.flatMap(lambda x: x[0])  # [address1, address2, ...]
candidate_volume = None




exit(0)
