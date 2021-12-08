



file_location = "/FileStore/tables/game_skater_stats.csv"
df = spark.read.format("csv").option("inferSchema",
           True).option("header", True).load(file_location)
display(df)



# e parquet format when working with Spark, because it is a file format that includes metadata about the column data types, offers file compression, and is a file format that is designed to work well with Spark.

# avro is also well designed to work with pyspark 


df.write.save('/FileStore/parquet/game_skater_stats',  
               format='parquet')
df = spark.read.load("/FileStore/parquet/game_skater_stats")
display(df)


# wildcard 

df = spark.read .load("s3a://my_bucket/game_skater_stats/*.parquet")






df_load = sparkSession.read.csv(‘hdfs://localhost:9000/myfiles/myfilename’)
df_load.show()




#write data

# DBFS (Parquet)
df.write.save('/FileStore/parquet/game_stats',format='parquet')

# S3 (Parquet)
df.write.parquet("s3a://my_bucket/game_stats", mode="overwrite")


# DBFS (CSV)
df.write.save('/FileStore/parquet/game_stats.csv', format='csv')
# discouraged as it pull data into a single node before writing 



