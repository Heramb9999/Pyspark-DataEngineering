import pyspark
from pyspark.sql import SparkSession

sparkcustom=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
list1=[1,2,3,4,5]
rdd1=sparkcustom.sparkContext.parallelize([list1])

print(f"The rdd stated is \n   {rdd1}")

print(f"CREATING A EMPTY RDD")
empty_rdd_crate=sparkcustom.sparkContext.emptyRDD()

print(f"The  empty rdd created is \n   {empty_rdd_crate}")
print("Stopping the spark objects")
sparkcustom.stop()

'''
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
The rdd stated is 
   ParallelCollectionRDD[0] at readRDDFromFile at PythonRDD.scala:274
CREATING A EMPTY RDD
The  empty rdd created is 
   EmptyRDD[1] at emptyRDD at <unknown>:0
Stopping the spark objects
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 12692 (child process of PID 18888) has been terminated.

'''