from pyspark.sql import SparkSession

with SparkSession.builder.appName("Reduce shuffle unoptimised").getOrCreate() as spark:
    parkViolations = spark.read.option("header", True).csv("/input/")

    plateTypeCountDF = parkViolations.groupBy("Plate Type").count()
    # used to show the plan before execution
    plateTypeCountDF.explain() 
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count")
    exit()