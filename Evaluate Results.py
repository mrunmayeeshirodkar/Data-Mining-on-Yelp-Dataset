import sys
import json
import time
import pandas as pd
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession \
    .builder \
    .getOrCreate()
#sqlContext = SQLContext(SparkContext)
ans1_file = sys.argv[1]
df1 = spark.read.json(ans1_file)
df1.createOrReplaceTempView("BIPFile")
#print(str(df1.show()))
sqlDF = spark.sql("SELECT business_id, name, city, stars, categories FROM BIPFile where business_id in ('4k3RlMAMd46DZ_JyZU0lMg', '7sPNbCx7vGAaH7SbNPZ6oA', 'JyxHvtj-syke7m9rbza7mA','4k3RlMAMd46DZ_JyZU0lMg', 'JyxHvtj-syke7m9rbza7mA', 'UPIYuRaZvknINOd1w8kqRQ','7sPNbCx7vGAaH7SbNPZ6oA', 'JyxHvtj-syke7m9rbza7mA', 'UPIYuRaZvknINOd1w8kqRQ','A5Rkh7UymKm0_Rxm9K2PJw', 'BxKe9Xt_fN6qBzrTofHuEQ', 'FaHADZARwnY4yvlvpnsfGA','A5Rkh7UymKm0_Rxm9K2PJw', 'BxKe9Xt_fN6qBzrTofHuEQ', 'gy-HBIeJGlQHs4RRYDLuHw','A5Rkh7UymKm0_Rxm9K2PJw', 'FaHADZARwnY4yvlvpnsfGA', 'gy-HBIeJGlQHs4RRYDLuHw','IMLrj2klosTFvPRLv56cng', 'igHYkXZMLAc9UdV5VnR_AA', 'qqs7LP4TXAoOrSlaKRfz3A','JyxHvtj-syke7m9rbza7mA', 'UPIYuRaZvknINOd1w8kqRQ', 'j5nPiTwWEFr-VsePew7Sjg','K7lWdNUhCbcnEvI0NhGewg', 'RESDUcs7fIiihp38-d6_6g', 'iCQpiavjjPzJ5_3gPD5Ebg')")
sqlDF.persist()
print(str(sqlDF.show()))
sqlDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("Results8.csv")