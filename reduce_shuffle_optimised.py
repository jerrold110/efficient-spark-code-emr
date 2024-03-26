from pyspark.sql import SparkSession

with SparkSession.builder.appName("Reduce shuffle optimised").getOrCreate() as spark:
    parkViolations = spark.read.option("header", True).csv("/input/")
    # THIS IS NOT A TECHNIQUE FOR SKEWED DATA BECAUSE OF A BOTTLENECK EXECUTOR, SALTING IS THE BEST APPROACH. MEDIUM ARTICLES ARE WRONG
    plateTypeCountDF = parkViolations.groupBy("Plate Type").count()
    plateTypeCountDF.explain() 

    # if we partition by the field Plate Type all the rows with similar Plate Type values end up in the same node.
    # 87 is the number of unique plate type values
    parkViolationsPlateTypeDF = parkViolations.repartition(87, "Plate Type") 
    parkViolationsPlateTypeDF.explain()
    plateTypeCountDF = parkViolationsPlateTypeDF.groupBy("Plate Type").count()
    plateTypeCountDF.explain()
    plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count.csv")
    exit()