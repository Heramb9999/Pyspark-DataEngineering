import pyspark
from pyspark.sql import SparkSession

sparkcustom=SparkSession.builder.master("local[4]").appName('this is transformations app').getOrCreate()
list1=[1,2,3,4,5,6,7,8,9,10]
rdd1=sparkcustom.sparkContext.parallelize(list1)
list2=[11,22,33,44,55,66,77,88,99,100]
rdd2=sparkcustom.sparkContext.parallelize(list2)

print(f"GETTING THE NO OF PARTITIONS DONE FOR A Rdd \n")
print(f"The no of partitions created for/ by the Rdd [rdd1] are : {rdd1.getNumPartitions()} :")
print(f"The no of partitions created for/ by the Rdd [rdd2] are : {rdd2.getNumPartitions()} :")
print(f"\n************************************************\n")
print(f"The current no of partitions created for/ by the Rdd [rdd2] are : {rdd1.getNumPartitions()} :")
print(f"\n Repartition TRANSFORMATION \n")
rdd1=rdd1.repartition(5)
print(f"The current no of partitions created for/ by the Rdd [rdd2] are : {rdd1.getNumPartitions()} :")
print(f"\n************************************************\n")
print(f"The current no of partitions created for/ by the Rdd [rdd2] are : {rdd1.getNumPartitions()} :")
print(f"\n Coalese TRANSFORMATION \n")
rdd1=rdd1.coalesce(2)
print(f"The current no of partitions created for/ by the Rdd [rdd2] are : {rdd1.getNumPartitions()} :")
print(f"\n************************************************\n")

print(f"APPLYING SOME TRANSFORMATIONS ON RDD -PYSPARK \n")
print(f"\n MAP TRANSFORMATION \n")
print(rdd1)
print(f"\n************************************************\n")
print(rdd1.map(lambda x: x + 10).collect())
print(f"\n************************************************\n")
print(rdd1.map(lambda x: x + 10).take(2))

print(f"\n FILTER TRANSFORMATION \n")
rdd_calc=rdd1.filter(lambda x:x%2==0)
print(f"\n************************************************\n")
print(rdd_calc)
print(f"\n************************************************\n")
print(rdd_calc.collect())
print(f"\n************************************************\n")
print(rdd_calc.take(2))

print(f"\n Union TRANSFORMATION \n")
unioned_rdd=rdd1.union(rdd2)
print(f"\n************************************************\n")
print(unioned_rdd)
print(f"\n************************************************\n")
print(unioned_rdd.take(2))


print(f"\n APPLYING SOME actions ON RDD -PYSPARK \n")
print(f"\n COLLECT ACTION \n")
print(f"\n************************************************\n")
print(rdd1.collect())
print(f"\n************************************************\n")
print(f"\n COUNT ACTION \n")
print(f"\n************************************************\n")
print(rdd1.count())
print(f"\n************************************************\n")
print(f"\n TAKE ACTION \n")
print(f"\n************************************************\n")
print(rdd1.take(3))
print(rdd1.take(2))
print(f"\n************************************************\n")




'''
OUTPUT

To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
GETTING THE NO OF PARTITIONS DONE FOR A Rdd 

The no of partitions created for/ by the Rdd [rdd1] are : 4 :
The no of partitions created for/ by the Rdd [rdd2] are : 4 :

************************************************

The current no of partitions created for/ by the Rdd [rdd2] are : 4 :

 Repartition TRANSFORMATION

The current no of partitions created for/ by the Rdd [rdd2] are : 5 :

************************************************

The current no of partitions created for/ by the Rdd [rdd2] are : 5 :

 Coalese TRANSFORMATION

The current no of partitions created for/ by the Rdd [rdd2] are : 2 :

************************************************

APPLYING SOME TRANSFORMATIONS ON RDD -PYSPARK


 MAP TRANSFORMATION

CoalescedRDD[7] at coalesce at <unknown>:0

************************************************

[11, 12, 15, 16, 17, 18, 19, 20, 13, 14]

************************************************

[11, 12]

 FILTER TRANSFORMATION


************************************************

PythonRDD[10] at RDD at PythonRDD.scala:53

************************************************

[2, 6, 8, 10, 4]

************************************************

[2, 6]

 Union TRANSFORMATION


************************************************

UnionRDD[15] at union at <unknown>:0

************************************************

[1, 2]

 APPLYING SOME actions ON RDD -PYSPARK


 COLLECT ACTION


************************************************

[1, 2, 5, 6, 7, 8, 9, 10, 3, 4]

************************************************


 COUNT ACTION


************************************************

10



 TAKE ACTION


************************************************

[1, 2, 5]
[1, 2]

************************************************

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 22392 (child process of PID 13160) has been terminated.
SUCCESS: The process with PID 13160 (child process of PID 11808) has been terminated.
SUCCESS: The process with PID 11808 (child process of PID 16620) has been terminated.

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> 


'''