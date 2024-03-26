from pyspark.sql import SparkSession
"""
Bucketing is a technique which you can use to repartition a dataframe based on a field. 
If you bucket both the dataframe based on the filed that they are supposed to be joined on, 
it will result in both the dataframes having their data chunks to be made available in the same nodes for joins, 
because the location of nodes are chosen using the hash of the partition field.
"""
with SparkSession.builder.appName("Bucket join optimised").getOrCreate() as spark:
    parkViolations_2015 = spark.read.option("header", True).csv("/input/2015.csv")
    parkViolations_2016 = spark.read.option("header", True).csv("/input/2016.csv")
    
    # rename all columns by replacing whitespace with _
    new_column_name_list= list(map(lambda x: x.replace(" ", "_"), parkViolations_2015.columns))
    parkViolations_2015 = parkViolations_2015.toDF(*new_column_name_list)
    parkViolations_2015 = parkViolations_2015.filter(parkViolations_2015.Plate_Type == "COM").filter(parkViolations_2015.Vehicle_Year == "2001")
    parkViolations_2016 = parkViolations_2016.toDF(*new_column_name_list)
    parkViolations_2016 = parkViolations_2016.filter(parkViolations_2016.Plate_Type == "COM").filter(parkViolations_2016.Vehicle_Year == "2001")

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) 
    # Writes dataframes into bucketed tables
    parkViolations_2015.write.mode("overwrite").bucketBy(400, "Vehicle_Year", "plate_type").saveAsTable("parkViolations_bkt_2015")
    parkViolations_2016.write.mode("overwrite").bucketBy(400, "Vehicle_Year", "plate_type").saveAsTable("parkViolations_bkt_2016")
    parkViolations_2015_tbl = spark.read.table("parkViolations_bkt_2015")
    parkViolations_2016_tbl = spark.read.table("parkViolations_bkt_2016")

    joinDF = parkViolations_2015_tbl.join(parkViolations_2016_tbl, (parkViolations_2015_tbl.Plate_Type ==  parkViolations_2016_tbl.Plate_Type) & (parkViolations_2015_tbl.Vehicle_Year ==  parkViolations_2016_tbl.Vehicle_Year) , "inner").select(parkViolations_2015_tbl["Summons_Number"], parkViolations_2016_tbl["Issue_Date"])
    # SortMergeJoin, but no exchange, which means no data shuffle
    joinDF.explain() # you will see SortMergeJoin
    joinDF.write.format("com.databricks.spark.csv").option("header", True).mode("overwrite").save("/output/bkt_joined_df.csv")
    exit()