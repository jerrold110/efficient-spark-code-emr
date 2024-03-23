parkViolations = spark.read.option("header", True).csv("/input/")
# THIS IS NOT A TECHNIQUE FOR SKEWED DATA, MEDIUM ARTICLES ARE WRONG. SALTING IS THE WAY TO GO

# Unoptimised code
plateTypeCountDF = parkViolations.groupBy("Plate Type").count()
plateTypeCountDF.explain() 

# optimsied code that distributes data by plate type
#if we partition by the field Plate Type all the rows with similar Plate Type values end up in the same node.
parkViolationsPlateTypeDF = parkViolations.repartition(87, "Plate Type") # number of unique plate type values
parkViolationsPlateTypeDF.explain() # you will see a filescan to read data and exchange hashpartition to shuffle and partition based on Plate Type
plateTypeCountDF = parkViolationsPlateTypeDF.groupBy("Plate Type").count()
plateTypeCountDF.explain() # check the execution plan, you will see the bottom 2 steps are for creating parkViolationsPlateTypeDF
plateTypeCountDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/plate_type_count.csv")