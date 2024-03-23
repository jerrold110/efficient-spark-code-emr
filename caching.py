parkViolations = spark.read.option("header", True).csv("/input/")
parkViolationsPlateTypeDF = parkViolations.repartition(87, "Plate Type")

# unoptimised code
plateTypeCountDF = parkViolationsPlateTypeDF.groupBy("Plate Type").count()
plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count.csv")
plateTypeAvgDF = parkViolationsPlateTypeDF.groupBy("Plate Type").avg() # avg is not meaningful here, but used just as an aggregation example
plateTypeAvgDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_avg.csv")

# optimised code with caching
cachedDF = parkViolationsPlateTypeDF.select('Plate Type').cache() # we are caching only the required field of the  dataframe in memory to keep cache size small
plateTypeCountDF = cachedDF.groupBy("Plate Type").count()
plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count.csv")
plateTypeAvgDF = cachedDF.groupBy("Plate Type").avg() # avg is not meaningful here, but used just as an aggregation example
plateTypeAvgDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_avg.csv")