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

"""
[
	{
		"id": "0001",
		"type": "donut",
		"name": "Cake",
		"ppu": 0.55,
		"batters":
			{
				"batter":
					[
						{ "id": "1001", "type": "Regular" },
						{ "id": "1002", "type": "Chocolate" },
						{ "id": "1003", "type": "Blueberry" },
						{ "id": "1004", "type": "Devil's Food" }
					]
			},
		"topping":
			[
				{ "id": "5001", "type": "None" },
				{ "id": "5002", "type": "Glazed" },
				{ "id": "5005", "type": "Sugar" },
				{ "id": "5007", "type": "Powdered Sugar" },
				{ "id": "5006", "type": "Chocolate with Sprinkles" },
				{ "id": "5003", "type": "Chocolate" },
				{ "id": "5004", "type": "Maple" }
			]
	},
	{
		"id": "0002",
		"type": "donut",
		"name": "Raised",
		"ppu": 0.55,
		"batters":
			{
				"batter":
					[
						{ "id": "1001", "type": "Regular" }
					]
			},
		"topping":
			[
				{ "id": "5001", "type": "None" },
				{ "id": "5002", "type": "Glazed" },
				{ "id": "5005", "type": "Sugar" },
				{ "id": "5003", "type": "Chocolate" },
				{ "id": "5004", "type": "Maple" }
			]
	},
	{
		"id": "0003",
		"type": "donut",
		"name": "Old Fashioned",
		"ppu": 0.55,
		"batters":
			{
				"batter":
					[
						{ "id": "1001", "type": "Regular" },
						{ "id": "1002", "type": "Chocolate" }
					]
			},
		"topping":
			[
				{ "id": "5001", "type": "None" },
				{ "id": "5002", "type": "Glazed" },
				{ "id": "5003", "type": "Chocolate" },
				{ "id": "5004", "type": "Maple" }
			]
	}
]

"""

# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\JSON DATASETS'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

multiline_df = sparkcustomobj.read.format('json').options(multiline=True).load('DATA ENGINEERING AND DATA ANALYSIS\JSON DATASETS\Cake_Nested_Json_Data.json')
print("**** multiline_df DATA****")
multiline_df.show(4)
print("Schema-of json df")
multiline_df.printSchema()
#PSSQLFUNC.explode(multiline_df["topping"].alias("topping_info_info"))

df_dataextract1=multiline_df.select(multiline_df["id"].alias("cake_id"),multiline_df["name"].alias("cake_name"),PSSQLFUNC.explode(multiline_df["batters.batter"]).alias("batters_info"),multiline_df["ppu"].alias("cake_ppu"),multiline_df["type"].alias("cake_type"),multiline_df["topping"].alias("topping_info"))

print("**** df_dataextract1 DATA****")
df_dataextract1.show(4)
print("Schema-of json df")
df_dataextract1.printSchema()

df_dataextract2=df_dataextract1.select(df_dataextract1["cake_id"]\
,df_dataextract1["cake_name"],df_dataextract1["batters_info"],df_dataextract1["cake_ppu"],df_dataextract1["cake_type"]\
,PSSQLFUNC.explode(df_dataextract1["topping_info"]).alias("topping_info_d"))
print("**** df_dataextract2 DATA****")
df_dataextract2.show(4)
print("Schema-of json df")
df_dataextract2.printSchema()

df_dataextract3=df_dataextract2.select(df_dataextract2["cake_id"]\
,df_dataextract2["cake_name"]\
,PSSQLFUNC.col("batters_info.*")\
,df_dataextract2["cake_ppu"],df_dataextract2["cake_type"]\
,PSSQLFUNC.col("topping_info_d.*"))


print("**** df_dataextract3 DATA****")
df_dataextract3.show(8)
print("Schema-of json df")
df_dataextract3.printSchema()

print("FINAL DATA OUTPUT AFTER FLATTENING THE JSON")
df_dataextract3.show(truncate=False)

df_data=df_dataextract1.select(df_dataextract1["batters_info.id"].alias("battery_id")   \
,df_dataextract1["batters_info.type"].alias("battery_type")         \
,df_dataextract1["topping_info.id"].alias("topping_id")           \
,df_dataextract1["topping_info.type"].alias("topping_type") )

print("BATTERY AND TOPPINGS EXTRACT DATA OUTPUT AFTER FLATTENING THE JSON")
df_data.show()

print("****END****")

"""
OUTPUT
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> & C:/Users/AsusT/AppData/Local/Programs/Python/Python310/python.exe "d:/HERAMB/IMP D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PYSPARK/PYSPARK_Ingest_NestedCake_JSON_03.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_Ingest_NestedCake_JSON_03.py  

**********
22/09/13 21:41:25 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
**** multiline_df DATA****
+--------------------+----+-------------+----+--------------------+-----+
|             batters|  id|         name| ppu|             topping| type|
+--------------------+----+-------------+----+--------------------+-----+
|{[{1001, Regular}...|0001|         Cake|0.55|[{5001, None}, {5...|donut|
| {[{1001, Regular}]}|0002|       Raised|0.55|[{5001, None}, {5...|donut|
|{[{1001, Regular}...|0003|Old Fashioned|0.55|[{5001, None}, {5...|donut|
+--------------------+----+-------------+----+--------------------+-----+

Schema-of json df
root
 |-- batters: struct (nullable = true)
 |    |-- batter: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- id: string (nullable = true)
 |    |    |    |-- type: string (nullable = true)
 |-- id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- ppu: double (nullable = true)
 |-- topping: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- type: string (nullable = true)
 |-- type: string (nullable = true)

**** df_dataextract1 DATA****
+-------+---------+--------------------+--------+---------+--------------------+
|cake_id|cake_name|        batters_info|cake_ppu|cake_type|        topping_info|
+-------+---------+--------------------+--------+---------+--------------------+
|   0001|     Cake|     {1001, Regular}|    0.55|    donut|[{5001, None}, {5...|
|   0001|     Cake|   {1002, Chocolate}|    0.55|    donut|[{5001, None}, {5...|
|   0001|     Cake|   {1003, Blueberry}|    0.55|    donut|[{5001, None}, {5...|
|   0001|     Cake|{1004, Devil's Food}|    0.55|    donut|[{5001, None}, {5...|
+-------+---------+--------------------+--------+---------+--------------------+
only showing top 4 rows

Schema-of json df
root
 |-- cake_id: string (nullable = true)
 |-- cake_name: string (nullable = true)
 |-- batters_info: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- type: string (nullable = true)
 |-- cake_ppu: double (nullable = true)
 |-- cake_type: string (nullable = true)
 |-- topping_info: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: string (nullable = true)
 |    |    |-- type: string (nullable = true)

**** df_dataextract2 DATA****
+-------+---------+---------------+--------+---------+--------------------+
|cake_id|cake_name|   batters_info|cake_ppu|cake_type|      topping_info_d|
+-------+---------+---------------+--------+---------+--------------------+
|   0001|     Cake|{1001, Regular}|    0.55|    donut|        {5001, None}|
|   0001|     Cake|{1001, Regular}|    0.55|    donut|      {5002, Glazed}|
|   0001|     Cake|{1001, Regular}|    0.55|    donut|       {5005, Sugar}|
|   0001|     Cake|{1001, Regular}|    0.55|    donut|{5007, Powdered S...|
+-------+---------+---------------+--------+---------+--------------------+
only showing top 4 rows

Schema-of json df
root
 |-- cake_id: string (nullable = true)
 |-- cake_name: string (nullable = true)
 |-- batters_info: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- type: string (nullable = true)
 |-- cake_ppu: double (nullable = true)
 |-- cake_type: string (nullable = true)
 |-- topping_info_d: struct (nullable = true)
 |    |-- id: string (nullable = true)
 |    |-- type: string (nullable = true)

**** df_dataextract3 DATA****
+-------+---------+----+---------+--------+---------+----+--------------------+
|cake_id|cake_name|  id|     type|cake_ppu|cake_type|  id|                type|
+-------+---------+----+---------+--------+---------+----+--------------------+
|   0001|     Cake|1001|  Regular|    0.55|    donut|5001|                None|
|   0001|     Cake|1001|  Regular|    0.55|    donut|5002|              Glazed|
|   0001|     Cake|1001|  Regular|    0.55|    donut|5005|               Sugar|
|   0001|     Cake|1001|  Regular|    0.55|    donut|5007|      Powdered Sugar|
|   0001|     Cake|1001|  Regular|    0.55|    donut|5006|Chocolate with Sp...|
|   0001|     Cake|1001|  Regular|    0.55|    donut|5003|           Chocolate|
|   0001|     Cake|1001|  Regular|    0.55|    donut|5004|               Maple|
|   0001|     Cake|1002|Chocolate|    0.55|    donut|5001|                None|
+-------+---------+----+---------+--------+---------+----+--------------------+
only showing top 8 rows

Schema-of json df
root
 |-- cake_id: string (nullable = true)
 |-- cake_name: string (nullable = true)
 |-- id: string (nullable = true)
 |-- type: string (nullable = true)
 |-- cake_ppu: double (nullable = true)
 |-- cake_type: string (nullable = true)
 |-- id: string (nullable = true)
 |-- type: string (nullable = true)

FINAL DATA OUTPUT AFTER FLATTENING THE JSON
+-------+---------+----+---------+--------+---------+----+------------------------+
|cake_id|cake_name|id  |type     |cake_ppu|cake_type|id  |type                    |
+-------+---------+----+---------+--------+---------+----+------------------------+
|0001   |Cake     |1001|Regular  |0.55    |donut    |5001|None                    |
|0001   |Cake     |1001|Regular  |0.55    |donut    |5002|Glazed                  |
|0001   |Cake     |1001|Regular  |0.55    |donut    |5005|Sugar                   |
|0001   |Cake     |1001|Regular  |0.55    |donut    |5007|Powdered Sugar          |
|0001   |Cake     |1001|Regular  |0.55    |donut    |5006|Chocolate with Sprinkles|
|0001   |Cake     |1001|Regular  |0.55    |donut    |5003|Chocolate               |
|0001   |Cake     |1001|Regular  |0.55    |donut    |5004|Maple                   |
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5001|None                    |
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5002|Glazed                  |
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5005|Sugar                   |
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5007|Powdered Sugar          |
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5006|Chocolate with Sprinkles|
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5003|Chocolate               |
|0001   |Cake     |1002|Chocolate|0.55    |donut    |5004|Maple                   |
|0001   |Cake     |1003|Blueberry|0.55    |donut    |5001|None                    |
|0001   |Cake     |1003|Blueberry|0.55    |donut    |5002|Glazed                  |
|0001   |Cake     |1003|Blueberry|0.55    |donut    |5005|Sugar                   |
|0001   |Cake     |1003|Blueberry|0.55    |donut    |5007|Powdered Sugar          |
|0001   |Cake     |1003|Blueberry|0.55    |donut    |5006|Chocolate with Sprinkles|
|0001   |Cake     |1003|Blueberry|0.55    |donut    |5003|Chocolate               |
+-------+---------+----+---------+--------+---------+----+------------------------+
only showing top 20 rows

BATTERY AND TOPPINGS EXTRACT DATA OUTPUT AFTER FLATTENING THE JSON
+----------+------------+--------------------+--------------------+
|battery_id|battery_type|          topping_id|        topping_type|
+----------+------------+--------------------+--------------------+
|      1001|     Regular|[5001, 5002, 5005...|[None, Glazed, Su...|
|      1002|   Chocolate|[5001, 5002, 5005...|[None, Glazed, Su...|
|      1003|   Blueberry|[5001, 5002, 5005...|[None, Glazed, Su...|
|      1004|Devil's Food|[5001, 5002, 5005...|[None, Glazed, Su...|
|      1001|     Regular|[5001, 5002, 5005...|[None, Glazed, Su...|
|      1001|     Regular|[5001, 5002, 5003...|[None, Glazed, Ch...|
|      1002|   Chocolate|[5001, 5002, 5003...|[None, Glazed, Ch...|
+----------+------------+--------------------+--------------------+

****END****
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 8848 (child process of PID 23932) has been terminated.
SUCCESS: The process with PID 23932 (child process of PID 19432) has been terminated.
SUCCESS: The process with PID 19432 (child process of PID 6788) has been terminated.

"""