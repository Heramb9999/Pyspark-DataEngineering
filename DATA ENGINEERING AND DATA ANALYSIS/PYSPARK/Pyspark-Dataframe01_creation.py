from msilib import schema
import pyspark # only run after findspark.init()
#findspark.init()
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

spark = SparkSession.builder.master('local[3]').getOrCreate()

empdatadict = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':['27', '24', '22', '32'],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}


empdatalist = [['Jai', 'Princi', 'Gaurav', 'Anuj'],
        ['27', '24', '22', '32'],
       ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        ['Msc', 'MA', 'MCA', 'Phd']]

print(f"\n************************************************\n")
print(f" PANDAS DATAFRAMES CREATED ")
print(pd.DataFrame(empdatadict))


print(f"\n************************************************\n")


empdatadict_schema = list(empdatadict.keys())
empdatadict_data = list(empdatadict.values())
rowformat_dictionary=list(zip(empdatadict_schema,empdatadict_data))
print(f"the zipped and converted dictionary  is --> {rowformat_dictionary}")

print(f" the dict is broken down into  \n{empdatadict_schema}  and \n {empdatadict_data}    \n")
print(f"The dictionary is as follows \n  {empdatadict}")

print(f"\n******WRONG OUTPUT START******************************************\n")
print(f" Observe the data,its wrong")

print(f"CREATING SPARK DATAFRAME FROM LIST ")
schemaforempdatalist=['Name','Age','Address','Qualification']
print(f"Creating spark dataframe from List \n")
pysparkdataframe01=spark.createDataFrame(data=empdatalist,schema=schemaforempdatalist)
print(f"\n************************************************\n")
print(type(pysparkdataframe01))
print(f"Spark dataframe from List  is as follows\n")
print(f"\n************************************************\n")
pysparkdataframe01.show()

print(f"CREATING SPARK DATAFRAME FROM DICTIONARY ")
pysparkdataframe02=spark.createDataFrame(data=empdatadict_data,schema=empdatadict_schema)
print(f"Spark dataframe from DICTIONARY EXTRACTED  is as follows\n")
pysparkdataframe02.show()
print(f"\n************************************************\n")


print(f"CREATING SPARK DATAFRAME FROM DICTIONARY -in wrong way ")
pysparkdataframe_correct=spark.createDataFrame(data=empdatadict_data,schema=empdatadict_schema)
print(pysparkdataframe_correct.show())
print(f"\n************************************************\n")
print(f"\n******WRONG OUTPUT END******************************************\n")

print(f"\n******CORRECT OUTPUT START******************************************\n")
print(f"Creating Spark dataframe from tuples with structured schema \n")
expdata2 = [("James","","Smith","36636","M",3000),
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
 
dffromstruct = spark.createDataFrame(data=expdata2,schema=schemastruct)
dffromstruct.printSchema()
print(f"Spark dataframe from tuples with structured schema \n")
dffromstruct.show(truncate=False)



print(f"Creating Spark dataframe from RDD \n")

#Create RDD from parallelize    
datalistrdd = [[1,2,3,4],[100,200,300,400]]
zippeddatardd=list(zip(datalistrdd[0],datalistrdd[1]))
rddfromlist=spark.sparkContext.parallelize(zippeddatardd)
print(f"rdd object -->{rddfromlist}")
print(f"displaying full  rdd content  --> \n {rddfromlist.collect()} ")
print(f"displaying some part of  rdd content  --> \n str({rddfromlist.take(2)} ")

print(f"CREATING DATAFRAME FROM RDD")


rddSchemast = StructType([       
    StructField("sales_id", IntegerType(), True),
     StructField("sales", IntegerType(), True)
])
DFFROMRDD=spark.createDataFrame(data=rddfromlist,schema=rddSchemast)
print(DFFROMRDD.show())
print(f"\n************************************************\n")

print(f"Creating Spark dataframe from PANDAS DATAFRAME \n")
pandasdf01=pd.DataFrame(empdatadict)
pysparkdf_from_pandasdf=spark.createDataFrame(pandasdf01)
print(f"\n************************************************\n")
print(pysparkdf_from_pandasdf)
print(f"\n************************************************\n")
print(pysparkdf_from_pandasdf.show())

print(f"\n******CORRECT OUTPUT END******************************************\n")

spark.stop()

'''
OUTPUT

22/01/23 14:15:21 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

************************************************

 PANDAS DATAFRAMES CREATED 
     Name Age    Address Qualification
0     Jai  27      Delhi           Msc
1  Princi  24     Kanpur            MA
2  Gaurav  22  Allahabad           MCA
3    Anuj  32    Kannauj           Phd

************************************************

the zipped and converted dictionary  is --> [('Name', ['Jai', 'Princi', 'Gaurav', 'Anuj']), ('Age', ['27', '24', '22', '32']), ('Address', ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj']), ('Qualification', ['Msc', 'MA', 'MCA', 'Phd'])]
 the dict is broken down into
['Name', 'Age', 'Address', 'Qualification']  and
 [['Jai', 'Princi', 'Gaurav', 'Anuj'], ['27', '24', '22', '32'], ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'], ['Msc', 'MA', 'MCA', 'Phd']]

The dictionary is as follows
  {'Name': ['Jai', 'Princi', 'Gaurav', 'Anuj'], 'Age': ['27', '24', '22', '32'], 'Address': ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'], 'Qualification': ['Msc', 'MA', 'MCA', 'Phd']}        

******WRONG OUTPUT START******************************************

 Observe the data,its wrong
CREATING SPARK DATAFRAME FROM LIST
Creating spark dataframe from List


************************************************

<class 'pyspark.sql.dataframe.DataFrame'>
Spark dataframe from List  is as follows


************************************************

+-----+------+---------+-------------+
| Name|   Age|  Address|Qualification|
+-----+------+---------+-------------+
|  Jai|Princi|   Gaurav|         Anuj|
|   27|    24|       22|           32|
|Delhi|Kanpur|Allahabad|      Kannauj|
|  Msc|    MA|      MCA|          Phd|
+-----+------+---------+-------------+

CREATING SPARK DATAFRAME FROM DICTIONARY
Spark dataframe from DICTIONARY EXTRACTED  is as follows

+-----+------+---------+-------------+
| Name|   Age|  Address|Qualification|
+-----+------+---------+-------------+
|  Jai|Princi|   Gaurav|         Anuj|
|   27|    24|       22|           32|
|Delhi|Kanpur|Allahabad|      Kannauj|
|  Msc|    MA|      MCA|          Phd|
+-----+------+---------+-------------+


************************************************

CREATING SPARK DATAFRAME FROM DICTIONARY -in wrong way
+-----+------+---------+-------------+
| Name|   Age|  Address|Qualification|
+-----+------+---------+-------------+
|  Jai|Princi|   Gaurav|         Anuj|
|   27|    24|       22|           32|
|Delhi|Kanpur|Allahabad|      Kannauj|
|  Msc|    MA|      MCA|          Phd|
+-----+------+---------+-------------+

None

************************************************


******WRONG OUTPUT END******************************************


******CORRECT OUTPUT START******************************************

Creating Spark dataframe from tuples with structured schema

root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)

Spark dataframe from tuples with structured schema

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|id   |gender|salary|
+---------+----------+--------+-----+------+------+
|James    |          |Smith   |36636|M     |3000  |
|Michael  |Rose      |        |40288|M     |4000  |
|Robert   |          |Williams|42114|M     |4000  |
|Maria    |Anne      |Jones   |39192|F     |4000  |
|Jen      |Mary      |Brown   |     |F     |-1    |
+---------+----------+--------+-----+------+------+

Creating Spark dataframe from RDD

rdd object -->ParallelCollectionRDD[28] at readRDDFromFile at PythonRDD.scala:274
displaying full  rdd content  --> 
 [(1, 100), (2, 200), (3, 300), (4, 400)]
displaying some part of  rdd content  --> 
 str([(1, 100), (2, 200)]
CREATING DATAFRAME FROM RDD
+--------+-----+
|sales_id|sales|
+--------+-----+
|       1|  100|
|       2|  200|
|       3|  300|
|       4|  400|
+--------+-----+

None

************************************************

Creating Spark dataframe from PANDAS DATAFRAME


************************************************

DataFrame[Name: string, Age: string, Address: string, Qualification: string]

************************************************

22/01/23 14:15:36 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
|  Name|Age|  Address|Qualification|
+------+---+---------+-------------+
|   Jai| 27|    Delhi|          Msc|
|Princi| 24|   Kanpur|           MA|
|Gaurav| 22|Allahabad|          MCA|
|  Anuj| 32|  Kannauj|          Phd|
+------+---+---------+-------------+

None

******CORRECT OUTPUT END******************************************

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 7748 (child process of PID 10756) has been terminated.
SUCCESS: The process with PID 10756 (child process of PID 10820) has been terminated.
SUCCESS: The process with PID 10820 (child process of PID 12424) has been terminated.

'''