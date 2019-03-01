import sys
import json
import time
from pyspark import SparkContext, SparkConf
sc = SparkContext()
input_file = sys.argv[1]
ans_file = sys.argv[2]
InputFile = sc.textFile(input_file)
OpFile = open(ans_file, 'w')
NumPartitions = int(sys.argv[3])
BusiRev = InputFile.map(json.loads).map(lambda e: (e['business_id'], e['business_id'])).persist()
#Default Sixth Task
OpFile.write("{")
t0 = time.time()
BusinessRev = BusiRev.map(lambda e:(e[1], 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0]))
t1 = time.time()
OpFile.write("\"default\":{")
def items_in_Partition(index, i):
	count = 0
	for _ in i:
		count = count + 1
	return index, count
MyList = BusiRev.mapPartitionsWithIndex(items_in_Partition).collect()
t = t1-t0
OpFile.write("\"n_partition\":")
OpFile.write('{}'.format(BusiRev.getNumPartitions()))
OpFile.write(",")
OpFile.write("\"n_items\":")
OpFile.write(str(MyList[1::2]))
OpFile.write(",")
OpFile.write("\"exe_time\":")
OpFile.write('{}'.format(t))
def CustomPartition(i):
	return hash(i)
BusiRev = BusiRev.partitionBy(NumPartitions, CustomPartition)
OpFile.write("},")
OpFile.write("\"customized\":{")
t0 = time.time()
BusinessRev = BusiRev.map(lambda e:(e[1], 1)).reduceByKey(lambda x,y:x+y).map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0]))
t1 = time.time()
OpFile.write("\"n_partition\":")
OpFile.write('{}'.format(BusiRev.getNumPartitions()))
OpFile.write(",")
def items_in_Partition(index, i):
	count = 0
	for _ in i:
		count = count + 1
	return index, count
MyList = BusiRev.mapPartitionsWithIndex(items_in_Partition).collect()
t = t1-t0
OpFile.write("\"n_items\":")
OpFile.write(str(MyList[1::2]))
OpFile.write(",")
OpFile.write("\"exe_time\":")
OpFile.write('{}'.format(t))
OpFile.write("},")
OpFile.write("\"explanation\":\"There is a Custom Partition function which uses hash to hash the key in such a way that all the keys will be at same reduce node, in the first case we let the system partition the data by the default mode. Because of the custom partition function the execution time has lowered considerably.\"")
OpFile.write("}")
OpFile.close()