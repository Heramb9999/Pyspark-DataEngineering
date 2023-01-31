
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import *
from pyspark.sql.window import *

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

states = {"NY":"New York", "CA":"California", "FL":"Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.show(truncate=False)

def state_convert(code):
    return broadcastStates.value[code]

def customlogic1(row):
    final_result=row[0]+"@"+row[1]+"@"+broadcastStates.value[row[3]]

result = df.rdd.map(lambda x: (x[0],x[1],x[2],state_convert(x[3]))).toDF(columns)
result.show(truncate=False)
print("*****************")
#this doesnot work
#df.select(concat(df["firstname"],"@",df["lastname"],"@",str(state_convert(df["state"])))).distinct().show(4)
#df.select(concat(df["firstname"],"@",df["lastname"],"@",str(broadcastStates.value[df["state"]])).distinct().show(4)

print("How to create a broadcast variable")
str1='Password'
broad_var=spark.sparkContext.broadcast(str1)

print("Accessing the broadcast variable")
print(f"accessed ->{broad_var.value}")


print("******end*******")

"""
OUTPUT

ANALYSIS/PYSPARK/PYSPARK_Broadcast_Variable.py"
22/09/05 15:53:23 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
root
 |-- firstname: string (nullable = true)
 |-- lastname: string (nullable = true) 
 |-- country: string (nullable = true)  
 |-- state: string (nullable = true)

+---------+--------+-------+-----+
|firstname|lastname|country|state|
+---------+--------+-------+-----+
|James    |Smith   |USA    |CA   |
|Michael  |Rose    |USA    |NY   |
|Robert   |Williams|USA    |CA   |
|Maria    |Jones   |USA    |FL   |
+---------+--------+-------+-----+

22/09/05 15:53:36 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
+---------+--------+-------+----------+
|firstname|lastname|country|state     |
+---------+--------+-------+----------+
|James    |Smith   |USA    |California|
|Michael  |Rose    |USA    |New York  |
|Robert   |Williams|USA    |California|
|Maria    |Jones   |USA    |Florida   |
+---------+--------+-------+----------+

*****************
How to create a broadcast variable
Accessing the broadcast variable
accessed ->Password
******end*******
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 22748 (child process of PID 5804) has been terminated.
SUCCESS: The process with PID 5804 (child process of PID 17608) has been terminated.
SUCCESS: The process with PID 17608 (child process of PID 10540) has been terminated.
"""