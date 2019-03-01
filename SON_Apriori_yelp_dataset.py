import sys
import json
import time
from itertools import combinations, chain
from pyspark import SparkContext, SparkConf
t1 = time.time()
sc = SparkContext()
Filter_Threshold = int(sys.argv[1])
Support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]
Frequent_Itemset1 = {}
Itemsets2 = {}
xm = []
Fre_It = {}
First_Iteration = 1

InputFile = sc.textFile(input_file).map(lambda e: e.split(",")).map(lambda e: (e[0],e[1]))
InputFile9 = InputFile.filter(lambda e: e[0] != "user_id")
InputFile.persist()
InputFile = InputFile9.groupByKey().map(lambda e: (set(e[1]))).filter(lambda e: len(e) > 70)
#InputFile = InputFile9.groupByKey().map(lambda e: (list(set(e[1])))).filter(lambda e: len(e) > 100)
#print(InputFile.take(1))
InputFile.persist()

def getKeys(Baskets, First_Iteration, all_items):
	#print(Baskets)
	Itemsets1 = {}
	comb2 = set()
	if First_Iteration > 1:
		comb = combinations(sorted(all_items), First_Iteration)
		for k in comb:
			comb2.add(k)
	#print(Baskets)	
	for basket in Baskets:
		if First_Iteration == 1:
			for item in basket:
				if item not in Itemsets1:
					Itemsets1[item] = 1
				else:
					Itemsets1[item] += 1
		else:
			for ik in comb2:
				if (set(ik).issubset(basket)):
						if ik not in Itemsets1:
							Itemsets1[ik] = 1
						else:
							Itemsets1[ik] += 1
	return Itemsets1

def MapOnePhaseOne(Baskets):
	basket2 = []
	all_items = set()
	First_Iteration = 1
	for basket in Baskets:
		basket2.append(basket)
	basket2 = tuple(basket2)
	while (len(Frequent_Itemset1.keys()) > 0) or (First_Iteration == 1):
		#print(len(Frequent_Itemset1.keys()))
		Itemsets1 = {}
		Itemsets1 = getKeys(basket2, First_Iteration, all_items)
		all_keys = set(Frequent_Itemset1.keys())
		for k in all_keys:
			del Frequent_Itemset1[k]
		kml = set()
		for item in Itemsets1:
			#print(item)
			if Itemsets1[item] >= Support/NumPartitions :
				#print("Its inside")
				Frequent_Itemset1[item] = Itemsets1[item]
				kml.add(item)
		if First_Iteration > 1:
			all_items = set(set().union(*kml))
		else:
			all_items = set(Frequent_Itemset1.keys())
		Fre_It[First_Iteration] = Frequent_Itemset1.copy()
		First_Iteration += 1
	return Fre_It.items()

def MapTwoPhaseTwo(Baskets):
	basket2 = []
	Itemset = {}
	iter = 1
	for basket in Baskets:
		basket2.append(basket)
	basket2 = tuple(basket2)
	for k in xm:
		for m in k[1]:
			cnt = 0
			for basket in basket2:
				if iter == 1:
					if m in basket:
						cnt += 1
				else:
					if (set(m).issubset(basket)):
						cnt += 1
			Itemset[m] = cnt
		iter += 1 
	return Itemset.items()

def ReduceOnePhaseOne(MapOutputOne):
	ReduceOutputOne = MapOutputOne.reduceByKey(lambda x,y : chain(set(x),set(y)))
	ReduceOutputOne = ReduceOutputOne.sortByKey().map(lambda e: (e[0], tuple(sorted(set(e[1])))))
	return ReduceOutputOne

def ReduceTwoPhaseTwo(MapOutputTwo, Support):
	MapOutputTwo = MapOutputTwo.reduceByKey(lambda x, y: x+y).filter(lambda e: e[1]>Support-1)
	return MapOutputTwo

def SONPhaseOne(Baskets):
	MapOutputOne = MapOnePhaseOne(Baskets)
	return MapOutputOne
	
def SONPhaseTwo(Baskets):
	MapOutputTwo = MapTwoPhaseTwo(Baskets)
	return MapOutputTwo

def items_in_Partition1(Baskets):
	PhaseOneOutput = SONPhaseOne(Baskets)
	return PhaseOneOutput
	
def items_in_Partition2(Baskets):
	PhaseTwoOutput = SONPhaseTwo(Baskets)
	return PhaseTwoOutput
	

NumPartitions = InputFile.getNumPartitions()

#------------------------------- It goes here
InputFile2 = InputFile.mapPartitions(items_in_Partition1)
ReduceOutputOne = ReduceOnePhaseOne(InputFile2)
xm = ReduceOutputOne.collect()

OutputFile = open(output_file, 'w')

iter = 1
OutputFile.write("Candidates:")
OutputFile.write("\n")
for k in xm:
	mi = 1
	#print(set(k))
	#print(" ")
	for m in k[1]:
		if iter == 1:
			OutputFile.write("('")
			OutputFile.write(str(m))
			OutputFile.write("')")
		else:
			OutputFile.write(str(m))
		if mi != len(k[1]):
			OutputFile.write(",")
		else:
			OutputFile.write("\n")
		mi += 1
	iter = 2
	OutputFile.write("\n")
InputFile3 = InputFile.mapPartitions(items_in_Partition2)
ReduceOutputTwo = ReduceTwoPhaseTwo(InputFile3, Support)
OutputFile.write("Frequent Itemsets:")
OutputFile.write("\n")
#print(ReduceOutputTwo.collect())
iter = 1
items = []
length = 0
for k in ReduceOutputTwo.collect():
	#print(str(type(m)))
	#if(str(type(m)) == "<class 'str'>"):
	if (type(k[0]) is str):
		items.append(k[0])
	if (type(k[0]) is tuple):
		if length < len(k[0]):
			length = len(k[0])
it = 0
for k in sorted(items):
	OutputFile.write("('")
	OutputFile.write(str(k))
	OutputFile.write("')")
	it += 1
	if it != len(items):
		OutputFile.write(",")
	else:
		OutputFile.write("\n")
		OutputFile.write("\n")
items = []
for m in range(2,length+1):
	for s in ReduceOutputTwo.collect():
		if (type(s[0]) is tuple):
			#print(s[0])
			if len(s[0]) == m:
				items.append(s[0])
	it = 0
	for k in sorted(items):
		OutputFile.write(str(k))
		it += 1
		if it != len(items):
			OutputFile.write(",")
		else:
			OutputFile.write("\n")
			OutputFile.write("\n")
	#print(" ")
	items = []
#OutputFile.write("Frequent_Itemset")
#OutputFile.write("\n")
#OutputFile.write(str(sorted(ReduceOutputTwo.collect())))
#OutputFile.write("\n")

#print(str(ReduceOutputTwo.take(1)))

#------------------------------------

t2 = time.time()
print("Duration:", str(t2-t1))
OutputFile.close()