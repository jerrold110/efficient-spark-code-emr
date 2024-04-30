## Techniques for optimising spark operations
This project is a personal exercise to understand methods for optimising spark operations by reducing data shuffle through various techniques. Data shuffle is the most expensive operation in a distributed system where data is transferred between nodes. When dealing with large amounts of data shuffling increasing waiting time. 

I write an optimised and unoptimised pyspark script for each technique and explore the difference in the way the spark execution engine processes the operations by looking at the DAG in the Spark history server in the Spark UI. SparkSession (DataFrames and SQL) is my preferred way of interacting with Spark, although I can work with SparkContext (RDD) as well, it is more troublesome.

### Environment
I set up HDFS and Spark environment on an Amazon EMR cluster then run bash commands to load the two 2gb datasets and file structure. I submit the pyspark scripts after creating an SSH tunnel into the master node of the EMR cluster, and open the Spark UI of the cluster in my local browser with port forwarding. https://repost.aws/knowledge-center/emr-access-spark-ui

## Techniques:
#### Select only the columns you need to reduce data shuffle
#### Repartitionong
We can reduce the data shuffle in groupby aggregate operations by moving the data that is processed together near the node where the computations take place. In this example I repartition the data by the field that they are grouped on in different nodes before performing the aggregate operation.

#### Caching
When using a data structure more than once, we can cache the data in memory to not have to read it into memory (and thus shuffle the data) more than one time.

#### Broadcast join
When joining a small dimension (mapping) table against a large fact table, there is a small to many join operation. We can reduce data shuffle of the large dataset by moving the data in the dimension table to each node that has the corresponding fact table data in it to prevent data shuffle of the large fact table.

#### Bucket join
If we need to join two large datasets and need all their columns. We can repartition the dataframes based on a field (bucketing) they are joined on so that both tables will have their data chunks in the same nodes for joining. This reduces the shuffle of moving data between nodes.