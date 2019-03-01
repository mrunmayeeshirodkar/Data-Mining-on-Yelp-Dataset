import sys
import json
import time
from pyspark import SparkContext, SparkConf
sc = SparkContext()
input_file = sys.argv[1]
ans1_file = sys.argv[3]
ans2_file = sys.argv[4]
argu3 = sys.argv[2]
IpFile1 = open(ans1_file, 'w')
IpFile2 = open(ans2_file, 'w')
InputFile = sc.textFile(input_file)
BIPFile = sc.textFile(argu3)
BusiStars = InputFile.map(json.loads).map(lambda row: (row['business_id'],row['user_id'])).persist()
BusiCity = BIPFile.map(json.loads).map(lambda row: (row['business_id'],row['state'])).persist()
db3 = BusiCity.join(BusiStars)
KL = db3.map(lambda x: x[1])
KL = KL.aggregateByKey((0,0), lambda x,y: (x[0] + y, x[1] + 1), lambda x,y: (x[0] + y[0], x[1] + y[1]))
t0 = time.time()
finalResult = KL.mapValues(lambda e: e[0]/e[1]).sortByKey(ascending=True).map(lambda e:(e[1],e[0])).sortByKey(ascending=False).map(lambda e:(e[1],e[0]))
t1 = time.time()
AT = finalResult.collect()
cnp = 0
for nl in AT:
	print(str(nl))
	cnp = cnp + 1
	if cnp == 10:
		break
t2 = time.time()
print(str(finalResult.take(10)))
t3 = time.time()
IpFile1.write("city,stars")
# IpFile1.write("\n")
for nl in AT:
	IpFile1.write("\n"+str(nl[0])+","+str(nl[1]))
IpFile2.write("{\"m1\":")
IpFile2.write(str(t2-t0))
IpFile2.write(",")
IpFile2.write("\"m2\":")
IpFile2.write(str(t1-t0 + t3-t2))
IpFile2.write(",")
IpFile2.write("\"explanation\":\"")
IpFile2.write("In method 1 I have used collect() method which gets the entire rdd to a master node causing usage of large memory and even though we print ten elements since it has the entire rdd it takes long time as compared to method 2. In method 2 I have used take() method which just takes the first 10 elements hence the execution time is less in the latter case.")
IpFile2.write("\"}")
IpFile1.close()
IpFile2.close()