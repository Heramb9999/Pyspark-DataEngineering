import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

accum=spark.sparkContext.accumulator(int(10))
rdd=spark.sparkContext.parallelize([1,2,3,4,5])
print(rdd.map(lambda x:x+1).collect())

print(rdd.collect()) #Accessed by driver
print(accum.value) #Accessed by driver
print(rdd.map(lambda x:accum.add(x)).collect())
print(f"current value of accumulator -->{accum.value}")
print(accum.add(100))
print(accum.value) #Accessed by driver

print("******END*****")

"""
OUTPUT
ANALYSIS/PYSPARK/PYSPARK_Accumulators_variable.py"
22/09/05 16:35:42 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
[2, 3, 4, 5, 6]
[1, 2, 3, 4, 5]
10
[None, None, None, None, None]
current value of accumulator -->25
None
125
******END*****
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 4572 (child process of PID 21512) has been terminated.
SUCCESS: The process with PID 21512 (child process of PID 9664) has been terminated.
SUCCESS: The process with PID 9664 (child process of PID 3832) has been terminated.

"""