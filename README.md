## Techniques for optimising spark operations
This project is a personal exercise to understand methods for optimising spark operations by reducing data shuffle through various techniques. Data shuffle is the most expensive operation in a distributed system where data is transferred between nodes. When dealing with large amounts of data shuffling increasing waiting time. 

I write an optimised and unoptimised pyspark script for each technique and explore the difference in the way the spark execution engine processes the operations by looking at the DAG in the Spark history server in the Spark UI. SparkSession (DataFrames and SQL) is my preferred way of interacting with Spark, although I can work with SparkContext (RDD) as well, it is more troublesome.

### Environment
I set up HDFS and Spark environment on an Amazon EMR cluster then run bash commands to load the two 2gb datasets and file structure. I submit the pyspark scripts after creating an SSH tunnel into the master node of the EMR cluster, and open the Spark UI of the cluster in my local browser with port forwarding. https://repost.aws/knowledge-center/emr-access-spark-ui

### Techniques to optimise code
* Reducing data shuffle in aggregate operations 
* Reducing data shuffle with caching
* Reducing data shuffle in join operations (multiple DataFrames)