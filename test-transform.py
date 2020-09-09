import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


# open spark session
spark = SparkSession.builder \
	.appName("test-transform") \
	.getOrCreate()

# open data frame
jsonFile = spark.read \
	.option("multiLine", "true") \
	.option("mode", "PERMISSIVE") \
	.json("test-input.json")

# show data
jsonFile.show()

# print schema
jsonFile.printSchema()

# get data frame with record id followed by flattened list of extended attributes so we can query against them
extAttList = jsonFile.select( "id", explode("extendedAttributes").alias("extAttList") )
extAttList.show()
extAttList.printSchema()

# get data frame with our final output values
finalOutput = extAttList.agg( \
	first(col("id")).alias("id"), \
	concat_ws(",",collect_list(col("extAttList.id"))).alias("extendedAttributeIds"), \
	concat_ws(",",collect_list(col("extAttList.attributeId"))).alias("extendedAttributeAttributeIds"), \
	concat_ws(",",collect_list(col("extAttList.text"))).alias("extendedAttributeTexts"), \
	concat_ws(",",collect_list(col("extAttList.dropDownListItem.id"))).alias("extendedAttributeListItemIds"), \
	concat_ws(",",collect_list(col("extAttList.dropDownListItem.customId"))).alias("extendedAttributeListItemCustomIds") )
finalOutput.show()
finalOutput.printSchema()

# send final output values to csv
if os.path.isdir("output.csv"):
	shutil.rmtree("output.csv")
finalOutput.write.format("csv").save("output.csv")
