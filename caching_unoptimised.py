from pyspark.sql import SparkSession

with SparkSession.builder.appName("Caching unoptimised").getOrCreate() as spark:

    parkViolations = spark.read.option("header", True).csv("/input/")
    parkViolationsPlateTypeDF = parkViolations.repartition(87, "Plate Type")
    
    plateTypeCountDF = parkViolationsPlateTypeDF.groupBy("Plate Type").count()
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count.csv")

    # Repartition happens a second time
    plateTypeAvgDF = parkViolationsPlateTypeDF.groupBy("Plate Type").avg()
    plateTypeAvgDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_avg.csv")
    exit()