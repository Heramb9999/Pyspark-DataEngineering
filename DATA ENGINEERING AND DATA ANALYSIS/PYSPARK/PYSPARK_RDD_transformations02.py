import pyspark
from pyspark.sql import SparkSession

print(f"Default Spark Session object is [spark]")
sparkcustom1=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()

list1=[1,2,3,4,5,6,7,8,9,10,11]
rdd1=sparkcustom1.sparkContext.parallelize(list1)
list2=[11,22,33,44,55,66,77,88,99,100,110]
rdd2=sparkcustom1.sparkContext.parallelize(list2)

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
print(rdd1.collect())
print(f"\n************************************************\n")
print(rdd1.map(lambda x: x + 10).collect())
print(f"\n************************************************\n")
print(rdd1.map(lambda x: x + 10).take(2))

print(f"\n FLATMAP TRANSFORMATION \n")
print(rdd1.collect())
print(f"\n************************************************\n")
d1 = ["This is an sample application to see the FlatMap operation in PySpark"]
rdd6 = sparkcustom1.sparkContext.parallelize(d1)
rdd10 = rdd6.flatMap(lambda x: x.split(" "))
print(f"\n************************************************\n")
print(f"FLATMAP can do this --> \n {rdd10.collect()}")
print(f"\n************************************************\n")
print(sparkcustom1.sparkContext.parallelize([3,6,5]).flatMap(lambda x: range(1,x)).collect())

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


"""
output

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

[1, 2, 7, 8, 9, 10, 11, 3, 4, 5, 6]

************************************************

[11, 12, 17, 18, 19, 20, 21, 13, 14, 15, 16]

************************************************

[11, 12]

 FLATMAP TRANSFORMATION

[1, 2, 7, 8, 9, 10, 11, 3, 4, 5, 6]

************************************************


************************************************

FLATMAP can do this --> 
 ['This', 'is', 'an', 'sample', 'application', 'to', 'see', 'the', 'FlatMap', 'operation', 'in', 'PySpark']

************************************************

[1, 2, 1, 2, 3, 4, 5, 1, 2, 3, 4]

 FILTER TRANSFORMATION


************************************************

PythonRDD[14] at RDD at PythonRDD.scala:53

************************************************

[2, 8, 10, 4, 6]

************************************************

[2, 8]

 Union TRANSFORMATION


************************************************

UnionRDD[19] at union at <unknown>:0

************************************************

[1, 2]

 APPLYING SOME actions ON RDD -PYSPARK


 COLLECT ACTION


************************************************

[1, 2, 7, 8, 9, 10, 11, 3, 4, 5, 6]

************************************************


 COUNT ACTION


************************************************

11

************************************************


 TAKE ACTION


************************************************

[1, 2, 7]
22/08/24 18:20:07 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
[1, 2]

************************************************

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 9324 (child process of PID 26872) has been terminated.
SUCCESS: The process with PID 26872 (child process of PID 27500) has been terminated.
SUCCESS: The process with PID 27500 (child process of PID 27536) has been terminated.



"""