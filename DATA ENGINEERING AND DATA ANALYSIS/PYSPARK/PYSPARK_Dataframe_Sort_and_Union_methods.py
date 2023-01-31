from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import concat,col

print(f"Default Spark Session object is [spark]")
spark=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
Sparkuserdefinedsession1=spark.newSession()

empdatadict = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':['27', '24', '22', '32'],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
panda_df=pd.DataFrame(empdatadict)

pysparkdf_from_pandasdf=spark.createDataFrame(panda_df)

print(f"Creating Spark dataframe from tuples with structured schema \n")
expdata_col_1 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schemastruct = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("id", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])

expdata_col_2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schemastruct2 = StructType([ 
    StructField("id", StringType(), True), 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("gender", StringType(), True),
     StructField("lastname",StringType(),True),
      StructField("salary", IntegerType(), True)
  ])

print(f"----DISPLAYING THE DATAFRAMES----")
print(f"\n************************************************\n") 
dffromstruct_col_01 = spark.createDataFrame(data=expdata_col_1,schema=schemastruct)
dffromstruct_col_01.printSchema()
dffromstruct_col_01.show()
print(f"\n************************************************\n") 
dffromstruct_col_02 = spark.createDataFrame(data=expdata_col_2,schema=schemastruct2)
dffromstruct_col_02.printSchema()
dffromstruct_col_02.show()

print(f"\n*****************single columns****************************\n") 
print(f"Pyspark dataframe Sorting methods /api")
dffromstruct_col_01.sort(dffromstruct_col_01['id'].asc()).show()
print(f"\n********************multiple columns************************\n") 
dffromstruct_col_01.sort(dffromstruct_col_01['salary'].desc(),dffromstruct_col_01['id'].asc()).show()
print(f"\n********************Sorting within partitions multiple columns************************\n") 
dffromstruct_col_01.sortWithinPartitions(dffromstruct_col_01['salary'].desc(),dffromstruct_col_01['id'].asc()).show()

print(f"Pyspark dataframe SET OPERATIONS methods /api")
dff_unioned_by_name=dffromstruct_col_01.unionByName(dffromstruct_col_02)
dff_unioned_by_name.show()


spark.stop()
Sparkuserdefinedsession1.stop()

'''
OUTPUT

Default Spark Session object is [spark]
22/01/23 18:18:44 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Creating Spark dataframe from tuples with structured schema 

----DISPLAYING THE DATAFRAMES----

************************************************

root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
|    Maria|      Anne|   Jones|39192|     F|  4000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+


************************************************

root
 |-- id: string (nullable = true)
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- salary: integer (nullable = true)

+-------+---------+----------+------+--------+------+
|     id|firstname|middlename|gender|lastname|salary|
+-------+---------+----------+------+--------+------+
|  James|         |     Smith| 36636|       M|  3000|
|Michael|     Rose|          | 40288|       M|  4000|
| Robert|         |  Williams| 42114|       M|  4000|
|  Maria|     Anne|     Jones| 39192|       F|  4000|
|    Jen|     Mary|     Brown|      |       F|    -1|
+-------+---------+----------+------+--------+------+


*****************single columns****************************

Pyspark dataframe Sorting methods /api
+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|      Jen|      Mary|   Brown|     |     F|    -1|
|    James|          |   Smith|36636|     M|  3000|
|    Maria|      Anne|   Jones|39192|     F|  4000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
+---------+----------+--------+-----+------+------+


********************multiple columns************************

22/01/23 18:18:58 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    Maria|      Anne|   Jones|39192|     F|  4000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
|    James|          |   Smith|36636|     M|  3000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+


********************Sorting within partitions multiple columns************************

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
|    Maria|      Anne|   Jones|39192|     F|  4000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+

Pyspark dataframe SET OPERATIONS methods /api
+---------+----------+--------+-------+------+------+
|firstname|middlename|lastname|     id|gender|salary|
|    James|          |   Smith|  36636|     M|  3000|
|  Michael|      Rose|        |  40288|     M|  4000|
|   Robert|          |Williams|  42114|     M|  4000|
|    Maria|      Anne|   Jones|  39192|     F|  4000|
|      Jen|      Mary|   Brown|       |     F|    -1|
|         |     Smith|       M|  James| 36636|  3000|
|     Rose|          |       M|Michael| 40288|  4000|
|         |  Williams|       M| Robert| 42114|  4000|
|     Anne|     Jones|       F|  Maria| 39192|  4000|
|     Mary|     Brown|       F|    Jen|      |    -1|
+---------+----------+--------+-------+------+------+

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 24636 (child process of PID 24852) has been terminated.
SUCCESS: The process with PID 24852 (child process of PID 26252) has been terminated.
SUCCESS: The process with PID 26252 (child process of PID 20076) has been terminated.
'''