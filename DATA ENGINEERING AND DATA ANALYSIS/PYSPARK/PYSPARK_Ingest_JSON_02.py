import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import *

print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\JSON DATASETS'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

multiline_df = sparkcustomobj.read.format('json').options(multiline=True).load('DATA ENGINEERING AND DATA ANALYSIS\JSON DATASETS\ECommerceShippingData.json')

"""
[
  {
    "ID": 1,
    "Warehouse_block": "D",
    "Mode_of_Shipment": "Flight",
    "Customer_care_calls": 4,
    "Customer_rating": 2,
    "Cost_of_the_Product": 177,
    "Prior_purchases": 3,
    "Product_importance": "low",
    "Gender": "F",
    "Discount_offered": 44,
    "Weight_in_gms": 1233,
    "Reached.on.Time_Y.N": 1
  },
  {
    "ID": 2,
    "Warehouse_block": "F",
    "Mode_of_Shipment": "Flight",
    "Customer_care_calls": 4,
    "Customer_rating": 5,
    "Cost_of_the_Product": 216,
    "Prior_purchases": 2,
    "Product_importance": "low",
    "Gender": "M",
    "Discount_offered": 59,
    "Weight_in_gms": 3088,
    "Reached.on.Time_Y.N": 1
  }
  ]



"""
print("Pbserve this was a simple json the inbuilt pyspark API formatted and flattened the JSON")
multiline_df.show(4)
print("Schema-of json df")
multiline_df.printSchema()

"""
OUTPUT
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> & C:/Users/AsusT/AppData/Local/Programs/Python/Python310/python.exe "d:/HERAMB/IMP D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PYSPARK/PYSPARK_Ingest_JSON_02.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_Ingest_JSON_02.py  

**********
22/09/13 21:39:13 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Pbserve this was a simple json the inbuilt pyspark API formatted and flattened the JSON
+-------------------+-------------------+---------------+----------------+------+---+----------------+---------------+------------------+-------------------+---------------+-------------+
|Cost_of_the_Product|Customer_care_calls|Customer_rating|Discount_offered|Gender| ID|Mode_of_Shipment|Prior_purchases|Product_importance|Reached.on.Time_Y.N|Warehouse_block|Weight_in_gms|
+-------------------+-------------------+---------------+----------------+------+---+----------------+---------------+------------------+-------------------+---------------+-------------+
|                177|                  4|              2|              44|     F|  1|          Flight|              3|               low|                  1|              D|         1233|
|                216|                  4|              5|              59|     M|  2|          Flight|              2|               low|                  1|              F|         3088|
|                183|                  2|              2|              48|     M|  3|          Flight|              4|               low|                  1|              A|         3374|
|                176|                  3|              3|              10|     M|  4|          Flight|              4|            medium|                  1|              B|         1177|
+-------------------+-------------------+---------------+----------------+------+---+----------------+---------------+------------------+-------------------+---------------+-------------+
only showing top 4 rows

Schema-of json df
root
 |-- Cost_of_the_Product: long (nullable = true)
 |-- Customer_care_calls: long (nullable = true)
 |-- Customer_rating: long (nullable = true)
 |-- Discount_offered: long (nullable = true)
 |-- Gender: string (nullable = true)
 |-- ID: long (nullable = true)
 |-- Mode_of_Shipment: string (nullable = true)
 |-- Prior_purchases: long (nullable = true)
 |-- Product_importance: string (nullable = true)
 |-- Reached.on.Time_Y.N: long (nullable = true)
 |-- Warehouse_block: string (nullable = true)
 |-- Weight_in_gms: long (nullable = true)

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 8756 (child process of PID 7928) has been terminated.
SUCCESS: The process with PID 7928 (child process of PID 12392) has been terminated.
SUCCESS: The process with PID 12392 (child process of PID 9868) has been terminated.
"""