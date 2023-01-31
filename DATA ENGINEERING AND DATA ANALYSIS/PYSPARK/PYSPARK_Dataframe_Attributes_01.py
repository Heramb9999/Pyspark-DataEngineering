from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

print("*************************")
print(f"NAME OF CURRENT PROGRAM IS --> {os.path.basename(__file__)}")
print("*************************")

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
print(f"\n************************************************\n")

print(f"EXPLORING THE VARIOUS ATTRIBUTES OF THE PYSPARK DATAFRAME \n")
print(f"\n Pyspark Dataframe columns Attribute \n")
print(pysparkdf_from_pandasdf.columns)
print(f"\n Pyspark Dataframe dtypes Attribute \n")
print(pysparkdf_from_pandasdf.dtypes)
print(f"\n Pyspark Dataframe isstreaming Attribute \n")
print(pysparkdf_from_pandasdf.isStreaming)
print(f"\n Pyspark Dataframe schema Attribute \n")
print(pysparkdf_from_pandasdf.schema)
print(f"\n Pyspark Dataframe storagelevel Attribute \n")
print(pysparkdf_from_pandasdf.storageLevel)

print("*** end *** ")
spark.stop()
Sparkuserdefinedsession1.stop()

"""
output

*************************
NAME OF CURRENT PROGRAM IS --> PYSPARK_Dataframe_Attributes_01.py
*************************
Default Spark Session object is [spark]
22/08/24 19:13:02 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
address of default spark object is 2605318996992 

address of user defined spark object is 2605318998240


************************************************

EXPLORING THE VARIOUS ATTRIBUTES OF THE PYSPARK DATAFRAME


 Pyspark Dataframe columns Attribute

['Name', 'Age', 'Address', 'Qualification']

 Pyspark Dataframe dtypes Attribute

[('Name', 'string'), ('Age', 'string'), ('Address', 'string'), ('Qualification', 'string')]

 Pyspark Dataframe isstreaming Attribute

False

 Pyspark Dataframe schema Attribute

StructType(List(StructField(Name,StringType,true),StructField(Age,StringType,true),StructField(Address,StringType,true),StructField(Qualification,StringType,true)))

 Pyspark Dataframe storagelevel Attribute

Serialized 1x Replicated
*** end ***
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 3360 (child process of PID 23656) has been terminated.
SUCCESS: The process with PID 23656 (child process of PID 16532) has been terminated.
SUCCESS: The process with PID 16532 (child process of PID 26912) has been terminated.


"""