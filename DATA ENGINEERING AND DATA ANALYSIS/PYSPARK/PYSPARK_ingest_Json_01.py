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

print(f"In this demo we will ingest and manipulate simple JSON-")
"""
{"store": {
    "name": "Sunshine Department Store",
    "address": "Wangfujing Street",
    "goods": {
        "book": [
            {
                "category": "Reference",
                "title": "Sayings of the Century",
                "author": "Nigel Rees",
                "price": 8.88
            },
            {
                "category": "Fiction",
                "title": "Sword of Honour",
                "author": "Evelyn Waugh",
                "price": 12.66
            }
        ],
        "bicycle": {
            "type": "GIANT OCR2600",
            "color": "White",
            "price": 276
        }
    }
}}
"""

multiline_df = sparkcustomobj.read.format('json').options(multiline=True).load('DATA ENGINEERING AND DATA ANALYSIS\JSON DATASETS\Talend Tool Store Json.json')
print("**********")
print("******print the schema of json-RAW FORMAT AS IT IS****")
multiline_df.printSchema()
print("******DATA*****")
multiline_df.show(truncate=False)  

print("**********")
multiline_df.select("store.*").show(truncate=False)
store_FLAT_DF_1=multiline_df.select("store.*")
print("******DATA*****")
store_FLAT_DF_1.select("goods.*").show(truncate=False)
print("Schema of Dataframe-")
store_FLAT_DF_1.printSchema()

print("******DATA*****")
store_json_df_2=store_FLAT_DF_1.select("goods.*")
print("Schema of Dataframe-")
store_json_df_2.printSchema()

print("**********")
data_extract_df_1=store_FLAT_DF_1.select(store_FLAT_DF_1["address"],store_FLAT_DF_1["goods.bicycle.color"].alias("bikecolour"),store_FLAT_DF_1["goods.bicycle.type"].alias("biketype"),store_FLAT_DF_1["goods.book"].alias("books_col"),store_FLAT_DF_1["name"].alias("store_name"))
print("******DATA*****")
data_extract_df_1.show(truncate=False)
print("Schema of Dataframe-")
data_extract_df_1.printSchema()

data_extract_df_2=data_extract_df_1.select(data_extract_df_1["address"],data_extract_df_1["bikecolour"],data_extract_df_1["biketype"],PSSQLFUNC.explode(data_extract_df_1["books_col"]).alias("Book_info"),data_extract_df_1["store_name"])

print("******DATA*****")
data_extract_df_2.show(truncate=False)
print("Schema of Dataframe-")
data_extract_df_2.printSchema()
print("**********")

data_extract_df_3=data_extract_df_2.select(data_extract_df_2["address"],data_extract_df_2["bikecolour"],data_extract_df_2["biketype"],data_extract_df_2["Book_info.author"].alias("Book_author"),data_extract_df_2["Book_info.title"].alias("Book_title"),data_extract_df_2["store_name"])
print("******DATA*****")
data_extract_df_3.show(truncate=False)
print("Schema of Dataframe-")
data_extract_df_3.printSchema()

print("Unfolding all")

data_extract_df_4=data_extract_df_2.select(data_extract_df_2["address"],data_extract_df_2["bikecolour"],data_extract_df_2["biketype"],"Book_info.*",data_extract_df_2["store_name"])
print("******DATA*****")
data_extract_df_4.show(truncate=False)
print("Schema of Dataframe-")
data_extract_df_4.printSchema()

print("******end******")

"""
OUTPUT
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> & C:/Users/AsusT/AppData/Local/Programs/Python/Python310/python.exe "d:/HERAMB/IMP D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PYSPARK/PYSPARK_ingest_Json_01.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_ingest_Json_01.py  

**********
22/09/13 16:22:10 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
In this demo we will ingest and manipulate simple JSON-
**********
******print the schema of json-RAW FORMAT AS IT IS****
root
 |-- store: struct (nullable = true)
 |    |-- address: string (nullable = true)
 |    |-- goods: struct (nullable = true)
 |    |    |-- bicycle: struct (nullable = true)
 |    |    |    |-- color: string (nullable = true)
 |    |    |    |-- price: long (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |    |    |-- book: array (nullable = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- author: string (nullable = true)
 |    |    |    |    |-- category: string (nullable = true)
 |    |    |    |    |-- price: double (nullable = true)
 |    |    |    |    |-- title: string (nullable = true)
 |    |-- name: string (nullable = true)

******DATA*****
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|store                                                                                                                                                                                  |   
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+   
|{Wangfujing Street, {{White, 276, GIANT OCR2600}, [{Nigel Rees, Reference, 8.88, Sayings of the Century}, {Evelyn Waugh, Fiction, 12.66, Sword of Honour}]}, Sunshine Department Store}|   
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+   

**********
+-----------------+---------------------------------------------------------------------------------------------------------------------------------------+-------------------------+
|address          |goods                                                                                                                                  |name                     |       
+-----------------+---------------------------------------------------------------------------------------------------------------------------------------+-------------------------+       
|Wangfujing Street|{{White, 276, GIANT OCR2600}, [{Nigel Rees, Reference, 8.88, Sayings of the Century}, {Evelyn Waugh, Fiction, 12.66, Sword of Honour}]}|Sunshine Department Store|       
+-----------------+---------------------------------------------------------------------------------------------------------------------------------------+-------------------------+       

******DATA*****
+---------------------------+--------------------------------------------------------------------------------------------------------+
|bicycle                    |book                                                                                                    |
+---------------------------+--------------------------------------------------------------------------------------------------------+
|{White, 276, GIANT OCR2600}|[{Nigel Rees, Reference, 8.88, Sayings of the Century}, {Evelyn Waugh, Fiction, 12.66, Sword of Honour}]|
+---------------------------+--------------------------------------------------------------------------------------------------------+

Schema of Dataframe-
root
 |-- address: string (nullable = true)
 |-- goods: struct (nullable = true)
 |    |-- bicycle: struct (nullable = true)
 |    |    |-- color: string (nullable = true)
 |    |    |-- price: long (nullable = true)
 |    |    |-- type: string (nullable = true)
 |    |-- book: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- author: string (nullable = true)
 |    |    |    |-- category: string (nullable = true)
 |    |    |    |-- price: double (nullable = true)
 |    |    |    |-- title: string (nullable = true)
 |-- name: string (nullable = true)

******DATA*****
Schema of Dataframe-
root
 |-- bicycle: struct (nullable = true)
 |    |-- color: string (nullable = true)
 |    |-- price: long (nullable = true)
 |    |-- type: string (nullable = true)
 |-- book: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- author: string (nullable = true)
 |    |    |-- category: string (nullable = true)
 |    |    |-- price: double (nullable = true)
 |    |    |-- title: string (nullable = true)

**********
******DATA*****
+-----------------+----------+-------------+--------------------------------------------------------------------------------------------------------+-------------------------+
|address          |bikecolour|biketype     |books_col                                                                                               |store_name               |
+-----------------+----------+-------------+--------------------------------------------------------------------------------------------------------+-------------------------+
|Wangfujing Street|White     |GIANT OCR2600|[{Nigel Rees, Reference, 8.88, Sayings of the Century}, {Evelyn Waugh, Fiction, 12.66, Sword of Honour}]|Sunshine Department Store|
+-----------------+----------+-------------+--------------------------------------------------------------------------------------------------------+-------------------------+

Schema of Dataframe-
root
 |-- address: string (nullable = true)
 |-- bikecolour: string (nullable = true)
 |-- biketype: string (nullable = true)
 |-- books_col: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- author: string (nullable = true)
 |    |    |-- category: string (nullable = true)
 |    |    |-- price: double (nullable = true)
 |    |    |-- title: string (nullable = true)
 |-- store_name: string (nullable = true)

******DATA*****
+-----------------+----------+-------------+-----------------------------------------------------+-------------------------+
|address          |bikecolour|biketype     |Book_info                                            |store_name               |
+-----------------+----------+-------------+-----------------------------------------------------+-------------------------+
|Wangfujing Street|White     |GIANT OCR2600|{Nigel Rees, Reference, 8.88, Sayings of the Century}|Sunshine Department Store|
|Wangfujing Street|White     |GIANT OCR2600|{Evelyn Waugh, Fiction, 12.66, Sword of Honour}      |Sunshine Department Store|
+-----------------+----------+-------------+-----------------------------------------------------+-------------------------+

Schema of Dataframe-
root
 |-- address: string (nullable = true)
 |-- bikecolour: string (nullable = true)
 |-- biketype: string (nullable = true)
 |-- Book_info: struct (nullable = true)
 |    |-- author: string (nullable = true)
 |    |-- category: string (nullable = true)
 |    |-- price: double (nullable = true)
 |    |-- title: string (nullable = true)
 |-- store_name: string (nullable = true)

**********
******DATA*****
+-----------------+----------+-------------+------------+----------------------+-------------------------+
|address          |bikecolour|biketype     |Book_author |Book_title            |store_name               |
+-----------------+----------+-------------+------------+----------------------+-------------------------+
|Wangfujing Street|White     |GIANT OCR2600|Nigel Rees  |Sayings of the Century|Sunshine Department Store|
|Wangfujing Street|White     |GIANT OCR2600|Evelyn Waugh|Sword of Honour       |Sunshine Department Store|
+-----------------+----------+-------------+------------+----------------------+-------------------------+

Schema of Dataframe-
root
 |-- address: string (nullable = true)
 |-- bikecolour: string (nullable = true)
 |-- biketype: string (nullable = true)
 |-- Book_author: string (nullable = true)
 |-- Book_title: string (nullable = true)
 |-- store_name: string (nullable = true)

Unfolding all
******DATA*****
+-----------------+----------+-------------+------------+---------+-----+----------------------+-------------------------+
|address          |bikecolour|biketype     |author      |category |price|title                 |store_name               |
+-----------------+----------+-------------+------------+---------+-----+----------------------+-------------------------+
|Wangfujing Street|White     |GIANT OCR2600|Nigel Rees  |Reference|8.88 |Sayings of the Century|Sunshine Department Store|
|Wangfujing Street|White     |GIANT OCR2600|Evelyn Waugh|Fiction  |12.66|Sword of Honour       |Sunshine Department Store|
+-----------------+----------+-------------+------------+---------+-----+----------------------+-------------------------+

Schema of Dataframe-
root
 |-- address: string (nullable = true)
 |-- bikecolour: string (nullable = true)
 |-- biketype: string (nullable = true)
 |-- author: string (nullable = true)
 |-- category: string (nullable = true)
 |-- price: double (nullable = true)
 |-- title: string (nullable = true)
 |-- store_name: string (nullable = true)

******end******
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 25224 (child process of PID 20628) has been terminated.

"""