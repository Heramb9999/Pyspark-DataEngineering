import pyspark
from pyspark.sql import SparkSession

print(f"Default Spark Session object is [spark]")
sparkcustom1=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()

list1=[1,2,3,4,5,6,7,8,9,10,11]
rdd1=sparkcustom1.sparkContext.parallelize(list1)
list2=[11,22,33,44,55,66,77,88,99,100,110]
rdd2=sparkcustom1.sparkContext.parallelize(list2)

"""
rdd.is_cached()-->returns true or false on the persist state of rdd
rdd.persisr() or rdd.persist(pyspark.StorageLevel.USERDEFINEDSTORAGELEVEL)
Ex-rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK_2)
rdd.cache()-It will persist the RDD cache can only persist RDD at Memory Only Storage level 
whereas persist() method can persist RDD at user defined storage level
rdd.unpersist()-It will unpersist the persisted rdd

"""


print("**********************************")
print("Checking the persist status of the RDD")
print(rdd1.is_cached)
print("persisiting the rdd1 ")
rdd1.persist()
print("**********************************")
print("Checking the persist status of the RDD")
print(f"rdd1 --> {rdd1.is_cached} and rdd2--> {rdd2.is_cached}")
print(f"**************Getting the Storage levels************")
print(f"\n rdd1-->{rdd1.getStorageLevel()} and rdd2-->{rdd2.getStorageLevel()} \n ")

print("persisiting the rdd2 ")
rdd2.cache()
print("Checking the persist status of the RDD")
print(f"rdd1 --> {rdd1.is_cached} and rdd2--> {rdd2.is_cached}")
print(f"**************Getting the Storage levels************")
print(f"\n rdd1-->{rdd1.getStorageLevel()} and rdd2-->{rdd2.getStorageLevel()} \n ")
print("Now we are UNpersisting the rdd1 using rdd.unpersist() method")
rdd1.unpersist()
rdd2.unpersist()
print("**********************************")
print("Checking the persist status of the RDD")
print(f"rdd1 --> {rdd1.is_cached} and rdd2--> {rdd2.is_cached}")

sparkcustom1.stop()

"""
OUTPUT
ERING AND DATA ANALYSIS/PYSPARK/PYSPARK_RDD_persist_and_cache.py"
Default Spark Session object is [spark]
22/08/24 17:47:09 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
**********************************
Checking the persist status of the RDD
False
persisiting the rdd1 
**********************************
Checking the persist status of the RDD
rdd1 --> True and rdd2--> False
**************Getting the Storage levels************

 rdd1-->Memory Serialized 1x Replicated and rdd2-->Serialized 1x Replicated

persisiting the rdd2
Checking the persist status of the RDD
rdd1 --> True and rdd2--> True
**************Getting the Storage levels************

 rdd1-->Memory Serialized 1x Replicated and rdd2-->Memory Serialized 1x Replicated

Now we are UNpersisting the rdd1 using rdd.unpersist() method
**********************************
Checking the persist status of the RDD
rdd1 --> False and rdd2--> False
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 18424 (child process of PID 26072) has been terminated.
SUCCESS: The process with PID 26072 (child process of PID 3736) has been terminated.
SUCCESS: The process with PID 3736 (child process of PID 15320) has been terminated.

"""