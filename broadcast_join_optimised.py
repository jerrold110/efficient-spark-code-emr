from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

with SparkSession.builder.appName("B join optimised").getOrCreate() as spark:
    parkViolations_2015 = spark.read.option("header", True).csv("/input/2015.csv")
    parkViolations_2016 = spark.read.option("header", True).csv("/input/2016.csv")
    parkViolations_2015 = parkViolations_2015.withColumnRenamed("Plate Type", "plateType")
    parkViolations_2016 = parkViolations_2016.withColumnRenamed("Plate Type", "plateType")
    """
    With broadcast join, Spark broadcast the smaller DataFrame to all executors and the executor keeps this DataFrame in memory and the 
    larger DataFrame is split and distributed across all executors so that Spark can perform a join without shuffling any data from the larger DataFrame 
    as the data required for join colocated on every executor.
    """
    parkViolations_2015_COM = parkViolations_2015.filter(parkViolations_2015.plateType == "COM").select("plateType", "Summons Number").distinct()
    parkViolations_2016_COM = parkViolations_2016.filter(parkViolations_2016.plateType == "COM").select("plateType", "Issue Date").distinct()
    # we can use broadcast join to move the mapping table to each of the node that has the fact tables data in it and preventing the data shuffle of the large dataset
    parkViolations_2015_COM.cache()
    parkViolations_2016_COM.cache()
    parkViolations_2015_COM.count() # will cause parkViolations_2015_COM to be cached
    parkViolations_2016_COM.count() # will cause parkViolations_2016_COM to be cached
    # Broadcast join
    joinDF = parkViolations_2015_COM\
        .join(broadcast(parkViolations_2016_COM), parkViolations_2015_COM.plateType ==  parkViolations_2016_COM.plateType, "inner")\
        .select(parkViolations_2015_COM["Summons Number"], parkViolations_2016_COM["Issue Date"])
    # BroadcastHashJoin
    joinDF.explain() 
    joinDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/joined_df")
    exit()