from pyspark.sql import SparkSession

with SparkSession.builder.appName("B join unoptimised").getOrCreate() as spark:
    parkViolations_2015 = spark.read.option("header", True).csv("/input/2015.csv")
    parkViolations_2016 = spark.read.option("header", True).csv("/input/2016.csv")
    parkViolations_2015 = parkViolations_2015.withColumnRenamed("Plate Type", "plateType")
    parkViolations_2016 = parkViolations_2016.withColumnRenamed("Plate Type", "plateType")

    parkViolations_2016_COM = parkViolations_2016.filter(parkViolations_2016.plateType == "COM")
    parkViolations_2015_COM = parkViolations_2015.filter(parkViolations_2015.plateType == "COM")
    # Data shuffle happens for both datasets
    joinDF = parkViolations_2015_COM\
        .join(parkViolations_2016_COM, parkViolations_2015_COM.plateType ==  parkViolations_2016_COM.plateType, "inner")\
        .select(parkViolations_2015_COM["Summons Number"], parkViolations_2016_COM["Issue Date"])
    # SortMergeJoin, with exchange for both dataframes, which means involves data shuffle of both dataframe
    joinDF.explain()
    joinDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/joined_df")
    exit()