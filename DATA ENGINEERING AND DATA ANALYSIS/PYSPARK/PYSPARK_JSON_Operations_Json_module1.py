import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import *
import json



print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\JSON DATASETS'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

print(f"In this demo we will ingest and manipulate simple JSON-")
print("Following is the sample json file")
"""
Talend Tool Store Json.Json --->file
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

print("USING VANILLA PYTHON JSON MODULE TO INGEST AND OPERATE ON THIS JSON FILE")

print("""
JSON MODULE OPERATIONS
DUMP-LOAD PYTHON OBJECT TO A JSON FILE 
DUMPS-LOAD PYTHON OBJECT TO A JSON OBJECT STRING 

LOAD-LOAD JSON DATA  FROM FILE INTO PYTHON OBJECT
LOAD-LOAD JSON DATA FROM PYTHON STRING TO PYTHON OBJECT
"""
)

JSON_STR='''
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
'''
print(f"we are using the following json string in this example \n {JSON_STR}")

with open("DATA ENGINEERING AND DATA ANALYSIS\JSON DATASETS\Talend Tool Store Json.json", "r") as read_file:  
     load_json_1=json.load(read_file)

print("*******content of the json file *********")
print(load_json_1)
print("*****************")
print(f"type of the data loaded is {type(load_json_1)}")
print("*****************")
load_json_2=json.loads(JSON_STR)
print("***************")
print(load_json_2)
print("*****************")
print(f"type of the data loaded is {type(load_json_2)}")


print("Navigate Json with Python json module")

address=load_json_2['store']['address']
books_data=load_json_2['store']['goods']['book']
bicycle_data=load_json_2['store']['goods']['bicycle']

print(f"Extracting the data from the python object(dictionary) -->{address}")
print(f"Extracting the data from the python object(dictionary) -->{books_data}")
print(f"Extracting the data from the python object(dictionary) -->{bicycle_data}")

customlist=[]
customlist.append(address)
customlist.extend(books_data)
customlist.extend(bicycle_data)
print("****************")
print(customlist)

json_data_dumped=json.dumps(load_json_1)
print("****************")
print(f"type of the data loaded is {type(json_data_dumped)}")


print("Working with JSON using pyspark")

multiline_df = sparkcustomobj.read.format('json').options(multiline=True).load('DATA ENGINEERING AND DATA ANALYSIS\JSON DATASETS\Talend Tool Store Json.json')

print("**********")
print("******print the schema of json-RAW FORMAT AS IT IS****")
print("----------------------------------------------")
print("*** multiline_df PRINT SCHEMA*****")
multiline_df.printSchema()
print("****** multiline_df DATA FROM THE DATAFRAME*****")
multiline_df.show(truncate=False)  
print("----------------------------------------------")
DF2=multiline_df.select(multiline_df['store.name'].alias('store_name'),multiline_df['store.address'].alias('store_address'),PSSQLFUNC.explode(multiline_df['store.goods.book']).alias('book'),PSSQLFUNC.col('store.goods.bicycle.*'))
print("*** DF2 PRINT SCHEMA*****")
DF2.printSchema()
print("****** DF2 DATA FROM THE DATAFRAME*****")
DF2.show(truncate=False)  
print("----------------------------------------------")

DF2_unfold1=multiline_df.select(multiline_df['store.name'].alias('store_name'),multiline_df['store.address'].alias('store_address')\
    ,PSSQLFUNC.explode(multiline_df['store.goods.book']).alias('book')\
    ,multiline_df['store.goods.bicycle.type'].alias('bicycle_type'),multiline_df['store.goods.bicycle.color'].alias('bicycle_color')\
        ,multiline_df['store.goods.bicycle.price'].alias('bicycle_price'))
print("*** DF2_unfold1 PRINT SCHEMA*****")
DF2_unfold1.printSchema()
print("****** DF2_unfold1 DATA FROM THE DATAFRAME*****")
DF2_unfold1.show(truncate=False)  
print("----------------------------------------------")
print("Accessing all the elements from STRUCT book with book.*")
dataf3=DF2.select(PSSQLFUNC.col("*"),PSSQLFUNC.col("book.*"))
print("*** dataf3 PRINT SCHEMA*****")
dataf3.printSchema()
print("***** dataf3 DATA FROM THE DATAFRAME*****")
dataf3.show(truncate=False)  

print("----------------------------------------------")
print("Accessing few selected  the elements from STRUCT book with json path")
dataf4=DF2.select(DF2['store_name'],DF2['book.title'].alias('bik_title'))
print("*** dataf4 PRINT SCHEMA*****")
dataf4.printSchema()
print("***** dataf4 DATA FROM THE DATAFRAME*****")
dataf4.show(truncate=False)  




print("******END******")

"""
output 
NALYSIS/PYSPARK/PYSPARK_JSON_Operations_Json_module1.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_JSON_Operations_Json_module1.py  

**********
22/10/18 18:41:44 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
In this demo we will ingest and manipulate simple JSON-
Following is the sample json file
USING VANILLA PYTHON JSON MODULE TO INGEST AND OPERATE ON THIS JSON FILE

JSON MODULE OPERATIONS
DUMP-LOAD PYTHON OBJECT TO A JSON FILE 
DUMPS-LOAD PYTHON OBJECT TO A JSON OBJECT STRING 

LOAD-LOAD JSON DATA  FROM FILE INTO PYTHON OBJECT
LOAD-LOAD JSON DATA FROM PYTHON STRING TO PYTHON OBJECT

we are using the following json string in this example

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

*******content of the json file *********
{'store': {'name': 'Sunshine Department Store', 'address': 'Wangfujing Street', 'goods': {'book': [{'category': 'Reference', 'title': 'Sayings of the Century', 'author': 'Nigel Rees', 'price': 8.88}, {'category': 'Fiction', 'title': 'Sword of Honour', 'author': 'Evelyn Waugh', 'price': 12.66}], 'bicycle': {'type': 'GIANT OCR2600', 'color': 'White', 'price': 276}}}}
*****************
type of the data loaded is <class 'dict'>
*****************
***************
{'store': {'name': 'Sunshine Department Store', 'address': 'Wangfujing Street', 'goods': {'book': [{'category': 'Reference', 'title': 'Sayings of the Century', 'author': 'Nigel Rees', 'price': 8.88}, {'category': 'Fiction', 'title': 'Sword of Honour', 'author': 'Evelyn Waugh', 'price': 12.66}], 'bicycle': {'type': 'GIANT OCR2600', 'color': 'White', 'price': 276}}}}
*****************
type of the data loaded is <class 'dict'>
Navigate Json with Python json module
Extracting the data from the python object(dictionary) -->Wangfujing Street
Extracting the data from the python object(dictionary) -->[{'category': 'Reference', 'title': 'Sayings of the Century', 'author': 'Nigel Rees', 'price': 8.88}, {'category': 'Fiction', 'title': 'Sword of Honour', 'author': 'Evelyn Waugh', 'price': 12.66}]
Extracting the data from the python object(dictionary) -->{'type': 'GIANT OCR2600', 'color': 'White', 'price': 276}
****************
['Wangfujing Street', {'category': 'Reference', 'title': 'Sayings of the Century', 'author': 'Nigel Rees', 'price': 8.88}, {'category': 'Fiction', 'title': 'Sword of Honour', 'author': 'Evelyn Waugh', 'price': 12.66}, 'type', 'color', 'price']
****************
type of the data loaded is <class 'str'>
Working with JSON using pyspark
**********
******print the schema of json-RAW FORMAT AS IT IS****
----------------------------------------------
*** multiline_df PRINT SCHEMA*****
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

****** multiline_df DATA FROM THE DATAFRAME*****
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|store                                                                                                                                                                                  |
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{Wangfujing Street, {{White, 276, GIANT OCR2600}, [{Nigel Rees, Reference, 8.88, Sayings of the Century}, {Evelyn Waugh, Fiction, 12.66, Sword of Honour}]}, Sunshine Department Store}|
+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

----------------------------------------------
*** DF2 PRINT SCHEMA*****
root
 |-- store_name: string (nullable = true)
 |-- store_address: string (nullable = true)
 |-- book: struct (nullable = true)
 |    |-- author: string (nullable = true)
 |    |-- category: string (nullable = true)
 |    |-- price: double (nullable = true)
 |    |-- title: string (nullable = true)
 |-- color: string (nullable = true)
 |-- price: long (nullable = true)
 |-- type: string (nullable = true)

****** DF2 DATA FROM THE DATAFRAME*****
+-------------------------+-----------------+-----------------------------------------------------+-----+-----+-------------+
|store_name               |store_address    |book                                                 |color|price|type         |
+-------------------------+-----------------+-----------------------------------------------------+-----+-----+-------------+
|Sunshine Department Store|Wangfujing Street|{Nigel Rees, Reference, 8.88, Sayings of the Century}|White|276  |GIANT OCR2600|
|Sunshine Department Store|Wangfujing Street|{Evelyn Waugh, Fiction, 12.66, Sword of Honour}      |White|276  |GIANT OCR2600|
+-------------------------+-----------------+-----------------------------------------------------+-----+-----+-------------+

----------------------------------------------
*** DF2_unfold1 PRINT SCHEMA*****
root
 |-- store_name: string (nullable = true)
 |-- store_address: string (nullable = true)
 |-- book: struct (nullable = true)
 |    |-- author: string (nullable = true)
 |    |-- category: string (nullable = true)
 |    |-- price: double (nullable = true)
 |    |-- title: string (nullable = true)
 |-- bicycle_type: string (nullable = true)
 |-- bicycle_color: string (nullable = true)
 |-- bicycle_price: long (nullable = true)

****** DF2_unfold1 DATA FROM THE DATAFRAME*****
+-------------------------+-----------------+-----------------------------------------------------+-------------+-------------+-------------+
|store_name               |store_address    |book                                                 |bicycle_type |bicycle_color|bicycle_price|
+-------------------------+-----------------+-----------------------------------------------------+-------------+-------------+-------------+
|Sunshine Department Store|Wangfujing Street|{Nigel Rees, Reference, 8.88, Sayings of the Century}|GIANT OCR2600|White        |276          |
|Sunshine Department Store|Wangfujing Street|{Evelyn Waugh, Fiction, 12.66, Sword of Honour}      |GIANT OCR2600|White        |276          |
+-------------------------+-----------------+-----------------------------------------------------+-------------+-------------+-------------+

----------------------------------------------
Accessing all the elements from STRUCT book with book.*
*** dataf3 PRINT SCHEMA*****
root
 |-- store_name: string (nullable = true)
 |-- store_address: string (nullable = true)
 |-- book: struct (nullable = true)
 |    |-- author: string (nullable = true)
 |    |-- category: string (nullable = true)
 |    |-- price: double (nullable = true)
 |    |-- title: string (nullable = true)
 |-- color: string (nullable = true)
 |-- price: long (nullable = true)
 |-- type: string (nullable = true)
 |-- author: string (nullable = true)
 |-- category: string (nullable = true)
 |-- price: double (nullable = true)
 |-- title: string (nullable = true)

***** dataf3 DATA FROM THE DATAFRAME*****
+-------------------------+-----------------+-----------------------------------------------------+-----+-----+-------------+------------+---------+-----+----------------------+
|store_name               |store_address    |book                                                 |color|price|type         |author      |category |price|title                 |
+-------------------------+-----------------+-----------------------------------------------------+-----+-----+-------------+------------+---------+-----+----------------------+
|Sunshine Department Store|Wangfujing Street|{Nigel Rees, Reference, 8.88, Sayings of the Century}|White|276  |GIANT OCR2600|Nigel Rees  |Reference|8.88 |Sayings of the Century|
|Sunshine Department Store|Wangfujing Street|{Evelyn Waugh, Fiction, 12.66, Sword of Honour}      |White|276  |GIANT OCR2600|Evelyn Waugh|Fiction  |12.66|Sword of Honour       |
+-------------------------+-----------------+-----------------------------------------------------+-----+-----+-------------+------------+---------+-----+----------------------+

----------------------------------------------
Accessing few selected  the elements from STRUCT book with json path
*** dataf4 PRINT SCHEMA*****
root
 |-- store_name: string (nullable = true)
 |-- bik_title: string (nullable = true)

***** dataf4 DATA FROM THE DATAFRAME*****
+-------------------------+----------------------+
|store_name               |bik_title             |
+-------------------------+----------------------+
|Sunshine Department Store|Sayings of the Century|
|Sunshine Department Store|Sword of Honour       |
+-------------------------+----------------------+

******END******
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 15852 (child process of PID 6056) has been terminated.
SUCCESS: The process with PID 6056 (child process of PID 26448) has been terminated.
SUCCESS: The process with PID 26448 (child process of PID 24072) has been terminated.

"""