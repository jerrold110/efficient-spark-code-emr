from pyspark.sql import SparkSession

with SparkSession.builder.appName("Caching optimised").getOrCreate() as spark:

    parkViolations = spark.read.option("header", True).csv("/input/")
    parkViolationsPlateTypeDF = parkViolations.repartition(87, "Plate Type")

    # caching prevents repartition from happening a second time
    # we are caching only the required field of the  dataframe in memory to keep cache size small
    cachedDF = parkViolationsPlateTypeDF.select('Plate Type').cache() 

    plateTypeCountDF = cachedDF.groupBy("Plate Type").count()
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count.csv")
    plateTypeAvgDF = cachedDF.groupBy("Plate Type").avg() 
    plateTypeAvgDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_avg.csv")