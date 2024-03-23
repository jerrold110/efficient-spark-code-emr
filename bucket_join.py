parkViolations_2015 = spark.read.option("header", True).csv("/input/2015.csv")
parkViolations_2016 = spark.read.option("header", True).csv("/input/2016.csv")

new_column_name_list= list(map(lambda x: x.replace(" ", "_"), parkViolations_2015.columns))

parkViolations_2015 = parkViolations_2015.toDF(*new_column_name_list)
parkViolations_2015 = parkViolations_2015.filter(parkViolations_2015.Plate_Type == "COM").filter(parkViolations_2015.Vehicle_Year == "2001")
parkViolations_2016 = parkViolations_2016.toDF(*new_column_name_list)
parkViolations_2016 = parkViolations_2016.filter(parkViolations_2016.Plate_Type == "COM").filter(parkViolations_2016.Vehicle_Year == "2001")
# we filter for COM and 2001 to limit time taken for the join

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) # we do this so that Spark does not auto optimize for broadcast join, setting to -1 means disable

parkViolations_2015.write.mode("overwrite").bucketBy(400, "Vehicle_Year", "plate_type").saveAsTable("parkViolations_bkt_2015")
parkViolations_2016.write.mode("overwrite").bucketBy(400, "Vehicle_Year", "plate_type").saveAsTable("parkViolations_bkt_2016")

parkViolations_2015_tbl = spark.read.table("parkViolations_bkt_2015")
parkViolations_2016_tbl = spark.read.table("parkViolations_bkt_2016")

joinDF = parkViolations_2015_tbl.join(parkViolations_2016_tbl, (parkViolations_2015_tbl.Plate_Type ==  parkViolations_2016_tbl.Plate_Type) & (parkViolations_2015_tbl.Vehicle_Year ==  parkViolations_2016_tbl.Vehicle_Year) , "inner").select(parkViolations_2015_tbl["Summons_Number"], parkViolations_2016_tbl["Issue_Date"])

joinDF.explain() # you will see SortMergeJoin, but no exchange, which means no data shuffle

# The below join will take a while, approx 30min
joinDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/bkt_joined_df.csv")
exit()