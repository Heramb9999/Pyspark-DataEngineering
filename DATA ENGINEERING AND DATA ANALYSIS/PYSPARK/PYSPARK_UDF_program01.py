from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import when
from pyspark.sql.functions import udf


print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

@udf(returnType=StringType())
def capitalize_the_second_letter1(data):
    datastr=data.replace(data[1],str(data[1]).upper(),1)
    return datastr


print("Program to demo how to use UDF in Pyspark")

print("Ingesting the products csv file")
products_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\products.csv')
print("**************")
print(products_from_df.columns)

print("USING UDF IN PYSPARK DATAFRAME- by wrapping under @udf ")
print("Select  distinct productname ,UDFforDataf(productname)  from data limit 4")
print(products_from_df.select(products_from_df["product_name"],capitalize_the_second_letter1(products_from_df["product_name"])).distinct().show(4))

print("USING UDF IN PYSPARK SQL -QUERY FORMAT AFTER REGISTERING IT")
print("Select  distinct productname ,UDFforDataf(productname)  from data limit 4")
products_from_df.createOrReplaceTempView("Products_table")
def capitalize_the_second_letter2(data):
    datastr=data.replace(data[3],str(data[3]).upper(),1)
    return datastr
#syntax sparkobj.udf.register("registereed_name",functionname_whichwecretaed(UDF),returntype)

sparkcustomobj.udf.register("capitalize_the_second_letter2_udf", capitalize_the_second_letter2,StringType())
sparkcustomobj.sql("select distinct product_name,capitalize_the_second_letter2_udf(product_name) from Products_table limit 4").show(4)

"""
OUTPUT
ANALYSIS/PYSPARK/PYSPARK_UDF_program01.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_UDF_program01.py  

**********
22/09/02 14:47:51 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Program to demo how to use UDF in Pyspark
Ingesting the products csv file
**************
['product_ID', 'product_type', 'product_name', 'size', 'colour', 'price', 'quantity', 'description']
USING UDF IN PYSPARK DATAFRAME- by wrapping under @udf 
Select  distinct productname ,UDFforDataf(productname)  from data limit 4
+---------------+-------------------------------------------+
|   product_name|capitalize_the_second_letter1(product_name)|
+---------------+-------------------------------------------+
|          Dress|                                      DRess|
|          Parka|                                      PArka|
|Casual Slim Fit|                            CAsual Slim Fit|
|          Cords|                                      COrds|
+---------------+-------------------------------------------+
only showing top 4 rows

None
USING UDF IN PYSPARK SQL -QUERY FORMAT AFTER REGISTERING IT
Select  distinct productname ,UDFforDataf(productname)  from data limit 4
+------------+-----------------------------------------------+
|product_name|capitalize_the_second_letter2_udf(product_name)|
+------------+-----------------------------------------------+
|     Peacoat|                                        PeaCoat|
|       Parka|                                          ParKa|
|      Bomber|                                         BomBer|
|       Cords|                                          CorDs|
+------------+-----------------------------------------------+

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 23052 (child process of PID 22224) has been terminated.

"""
