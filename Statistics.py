import sys
import json
import time
from pyspark import SparkContext, SparkConf
sc = SparkContext()
t7 = time.time()
input_file = sys.argv[1]
ans_file = sys.argv[2]
InputFile = sc.textFile(input_file)
OpFile = open(ans_file, 'w')
#First Task
dataset = InputFile.map(json.loads).map(lambda r: (r['business_id'],r['user_id'],r['date']))
dataset.persist()
OpFile.write("{")
OpFile.write("\"n_review\":")
OpFile.write('{}'.format(dataset.count()))
OpFile.write(",")
#Second Task
OpFile.write("\"n_review_2018\":")
Rev = dataset.map(lambda r: (r[2])).filter(lambda val: val[:4] == "2018")
OpFile.write('{}'.format(Rev.count()))
OpFile.write(",")
#Third Task
OpFile.write("\"n_user\":")
UserRev = dataset.map(lambda r: (r[1]))
DisUser = UserRev.distinct()
OpFile.write('{}'.format(DisUser.count()))
OpFile.write(",")
#Fourth Task
OpFile.write("\"top10_user\": [")
UsersRev = UserRev.map(lambda e:(e, 1)).reduceByKey(lambda x,y:x+y).sortByKey(ascending=True).map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0]))
cnt = 0
for nm in UsersRev.take(10):
	cnt += 1
	OpFile.write("[\"")
	OpFile.write(str(nm[0]))
	OpFile.write("\",")
	OpFile.write('{}'.format(nm[1]))
	OpFile.write("]")
	if cnt != 10:
		OpFile.write(",")
OpFile.write("],")
#Fifth Task
OpFile.write("\"n_business\":")
BusiRev = dataset.map(lambda r: (r[0]))
DisBusi = BusiRev.distinct()
OpFile.write('{}'.format(DisBusi.count()))
OpFile.write(",")
#Sixth Task
OpFile.write("\"top10_business\": [")
BusinessRev = BusiRev.map(lambda e:(e, 1)).reduceByKey(lambda x,y:x+y).sortByKey(ascending=True).map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0]))
cnt = 0
for nm in BusinessRev.take(10):
	cnt += 1
	OpFile.write("[\"")
	OpFile.write(str(nm[0]))
	OpFile.write("\",")
	OpFile.write('{}'.format(nm[1]))
	OpFile.write("]")
	if cnt != 10:
		OpFile.write(",")
OpFile.write("]")
OpFile.write("}")
t6 = time.time()
# OpFile.write(str(t6-t7))
OpFile.close()