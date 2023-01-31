from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import concat,col

print(f"Default Spark Session object is [spark]")
spark=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
print(f"address of default spark object is {id(spark)} \n")
Sparkuserdefinedsession1=spark.newSession()
print(f"address of user defined spark object is {id(Sparkuserdefinedsession1)} \n")


data_eng_emp = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]
columns = ["firstname","lastname","country","state"]

empdatadict = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':['27', '24', '22', '32'],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
panda_df=pd.DataFrame(empdatadict)

pysparkdf_from_pandasdf=spark.createDataFrame(panda_df)

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

print(f"\n************************************************\n") 
dffromstruct = spark.createDataFrame(data=expdata2,schema=schemastruct)
dffromstruct.printSchema()


print(f"\n************************************************\n")
print(f"EXPLORING THE VARIOUS ATTRIBUTES OF THE PYSPARK DATAFRAME \n")
print(f"\n Pyspark Dataframe [ SELECT ] Method or API \n")
dffromstruct.select(['firstname','middlename']).show()
dffromstruct.select(concat(dffromstruct.firstname,dffromstruct.middlename,dffromstruct.lastname).alias('fullname'),dffromstruct.salary,(dffromstruct.salary*2).alias('ExpectedSalary')).show()
dffromstruct.select(concat(dffromstruct["firstname"],dffromstruct["middlename"],dffromstruct["lastname"]).alias('fullname'),dffromstruct.salary,(dffromstruct.salary*2).alias('ExpectedSalary')).show()
print(f"\n************************************************\n")
print(f"\n Pyspark Dataframe [ With column ] Method or API \n")
dffromstruct=dffromstruct.withColumn('full_name',concat(dffromstruct.firstname,dffromstruct.middlename,dffromstruct.lastname))
#another way to write it
dffromstruct01=dffromstruct.withColumn('full_name',concat(dffromstruct["firstname"],dffromstruct["middlename"],dffromstruct["lastname"]))
dffromstruct.show()
dffromstruct01.show()
print(f"\n************************************************\n")
print(f"\n Pyspark Dataframe [ drop ] column Method or API \n")
dffromstruct=dffromstruct.drop('full_name')
dffromstruct.show()

print(f"\n************************************************\n")
print(f"\n Pyspark Dataframe [ dropDuplicates ] column Method or API \n")
dffromstruct=dffromstruct.drop_duplicates()
dffromstruct.show()
print(f"Dropping duplicates on a specific set")
dffromstruct_subsetdropped=dffromstruct.drop_duplicates(['salary'])
dffromstruct_subsetdropped.show()

print(f"\n************************************************\n")
print(f"\n Pyspark Dataframe [ filter ] column Method or API \n")
dffromstruct.filter(dffromstruct.gender=='M').show()
print(f"\n************************************************\n")
dffromstruct.filter(dffromstruct['gender']=='M').show()
print(f"\n************************************************\n")
dffromstruct.filter((dffromstruct['gender']=='M') & (dffromstruct['firstname']=='Michael')).show()
print(f"\n************************************************\n")
dffromstruct.filter((dffromstruct['gender']=='M') & (dffromstruct['firstname'].isin('Michael','Robert'))).show()

spark.stop()
Sparkuserdefinedsession1.stop()

'''
OUTPUT
Default Spark Session object is [spark]
22/01/23 17:34:46 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
address of default spark object is 2863101314720 

address of user defined spark object is 2863101315968 

Creating Spark dataframe from tuples with structured schema 


************************************************

root
 |-- firstname: string (nullable = true)
 |-- middlename: string (nullable = true)
 |-- lastname: string (nullable = true)
 |-- id: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- salary: integer (nullable = true)


************************************************

EXPLORING THE VARIOUS ATTRIBUTES OF THE PYSPARK DATAFRAME


 Pyspark Dataframe [ SELECT ] Method or API

+---------+----------+
|firstname|middlename|
+---------+----------+
|    James|          |
|  Michael|      Rose|
|   Robert|          |
|    Maria|      Anne|
|      Jen|      Mary|
+---------+----------+

+--------------+------+--------------+
|      fullname|salary|ExpectedSalary|
+--------------+------+--------------+
|    JamesSmith|  3000|          6000|
|   MichaelRose|  4000|          8000|
|RobertWilliams|  4000|          8000|
|MariaAnneJones|  4000|          8000|
|  JenMaryBrown|    -1|            -2|
+--------------+------+--------------+

+--------------+------+--------------+
|      fullname|salary|ExpectedSalary|
+--------------+------+--------------+
|    JamesSmith|  3000|          6000|
|   MichaelRose|  4000|          8000|
|RobertWilliams|  4000|          8000|
|MariaAnneJones|  4000|          8000|
|  JenMaryBrown|    -1|            -2|
+--------------+------+--------------+


************************************************


 Pyspark Dataframe [ With column ] Method or API

+---------+----------+--------+-----+------+------+--------------+
|firstname|middlename|lastname|   id|gender|salary|     full_name|
+---------+----------+--------+-----+------+------+--------------+
|    James|          |   Smith|36636|     M|  3000|    JamesSmith|
|  Michael|      Rose|        |40288|     M|  4000|   MichaelRose|
|   Robert|          |Williams|42114|     M|  4000|RobertWilliams|
|    Maria|      Anne|   Jones|39192|     F|  4000|MariaAnneJones|
|      Jen|      Mary|   Brown|     |     F|    -1|  JenMaryBrown|
+---------+----------+--------+-----+------+------+--------------+

+---------+----------+--------+-----+------+------+--------------+
|firstname|middlename|lastname|   id|gender|salary|     full_name|
+---------+----------+--------+-----+------+------+--------------+
|    James|          |   Smith|36636|     M|  3000|    JamesSmith|
|  Michael|      Rose|        |40288|     M|  4000|   MichaelRose|
|   Robert|          |Williams|42114|     M|  4000|RobertWilliams|
|    Maria|      Anne|   Jones|39192|     F|  4000|MariaAnneJones|
|      Jen|      Mary|   Brown|     |     F|    -1|  JenMaryBrown|
+---------+----------+--------+-----+------+------+--------------+


************************************************


 Pyspark Dataframe [ drop ] column Method or API

22/01/23 17:35:02 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
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


 Pyspark Dataframe [ dropDuplicates ] column Method or API

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|    Maria|      Anne|   Jones|39192|     F|  4000|
|   Robert|          |Williams|42114|     M|  4000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+

Dropping duplicates on a specific set
+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|      Jen|      Mary|   Brown|     |     F|    -1|
+---------+----------+--------+-----+------+------+


************************************************


 Pyspark Dataframe [ filter ] column Method or API

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
+---------+----------+--------+-----+------+------+


************************************************

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|    James|          |   Smith|36636|     M|  3000|
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
+---------+----------+--------+-----+------+------+


************************************************

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
+---------+----------+--------+-----+------+------+


************************************************

+---------+----------+--------+-----+------+------+
|firstname|middlename|lastname|   id|gender|salary|
+---------+----------+--------+-----+------+------+
|  Michael|      Rose|        |40288|     M|  4000|
|   Robert|          |Williams|42114|     M|  4000|
+---------+----------+--------+-----+------+------+

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 3120 (child process of PID 26456) has been terminated.
SUCCESS: The process with PID 26456 (child process of PID 22148) has been terminated.
SUCCESS: The process with PID 22148 (child process of PID 3984) has been terminated.


'''