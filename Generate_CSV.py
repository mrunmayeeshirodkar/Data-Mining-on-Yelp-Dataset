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
ans2_file = sys.argv[2]
output_file = sys.argv[3]
df1 = spark.read.json(ans1_file)
df2 = spark.read.json(ans2_file)
df1 = df1.select(df1['user_id'], df1['business_id'])
df2 = df2.select(df2['business_id'], df2['state'])
df1.createOrReplaceTempView("BIPFile")
df2.createOrReplaceTempView("BusiCity")
print("-----------------------------")
print("-----------------------------")
print("-----------------------------")
print("-----------------------------")
print("-----------------------------")
print("-----------------------------")
#result = pd.concat([df1, df4], axis=1, join='inner')
#sqlDF = spark.sql("SELECT a.user_id, a.business_id, b.state FROM BIPFile a UNION BusiCity b WHERE b.state = 'NV' and a.business_id = b.business_id)")
sqlDF = df1.join(df2, df1["business_id"] == df2["business_id"]).drop(df2["business_id"])
sqlDF.createOrReplaceTempView("MyTable")
print(str(sqlDF.show()))
#sqlDF = spark.sql("SELECT user_id, business_id, state FROM BIPFile JOIN BusiCity WHERE state = 'NV' and business_id = business_id)")
sqlDF = spark.sql("SELECT user_id, business_id FROM MyTable where state = 'NV'")
sqlDF.persist()
print(str(sqlDF.show()))
print(str(df2.show()))
sqlDF.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("mydata5.csv")
#BIPFile = sc.textFile(ans2_file)
#BusiStars = InputFile.map(json.loads).map(lambda row: (row['user_id'],row['business_id'])).map(lambda x: Row(user_id=x[0], business_id=x[1]))
#BusiCity = BIPFile.map(json.loads).map(lambda row: (row['business_id'],row['city'])).map(lambda x: Row(business_id=x[0], city= x[1]))
#print(str(BusiStars.(5)))