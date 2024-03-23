parkViolations_2015 = spark.read.option("header", True).csv("/input/2015.csv")
parkViolations_2016 = spark.read.option("header", True).csv("/input/2016.csv")

parkViolations_2015 = parkViolations_2015.withColumnRenamed("Plate Type", "plateType") # simple column rename for easier joins
parkViolations_2016 = parkViolations_2016.withColumnRenamed("Plate Type", "plateType")

# unoptimised code

parkViolations_2016_COM = parkViolations_2016.filter(parkViolations_2016.plateType == "COM")
parkViolations_2015_COM = parkViolations_2015.filter(parkViolations_2015.plateType == "COM")

joinDF = parkViolations_2015_COM.join(parkViolations_2016_COM, parkViolations_2015_COM.plateType ==  parkViolations_2016_COM.plateType, "inner").select(parkViolations_2015_COM["Summons Number"], parkViolations_2016_COM["Issue Date"])
joinDF.explain() # you will see SortMergeJoin, with exchange for both dataframes, which means involves data shuffle of both dataframe
# The below join will take a very long time with the given infrastructure, do not run, unless needed
# joinDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/joined_df")
#exit()


# optimsed code with broadcast join
parkViolations_2015_COM = parkViolations_2015.filter(parkViolations_2015.plateType == "COM").select("plateType", "Summons Number").distinct()
parkViolations_2016_COM = parkViolations_2016.filter(parkViolations_2016.plateType == "COM").select("plateType", "Issue Date").distinct()

parkViolations_2015_COM.cache()
parkViolations_2016_COM.cache()

parkViolations_2015_COM.count() # will cause parkViolations_2015_COM to be cached
parkViolations_2016_COM.count() # will cause parkViolations_2016_COM to be cached

joinDF = parkViolations_2015_COM.join(parkViolations_2016_COM.hint("broadcast"), parkViolations_2015_COM.plateType ==  parkViolations_2016_COM.plateType, "inner").select(parkViolations_2015_COM["Summons Number"], parkViolations_2016_COM["Issue Date"])
joinDF.explain() # you will see BroadcastHashJoin

joinDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/joined_df")
exit()

