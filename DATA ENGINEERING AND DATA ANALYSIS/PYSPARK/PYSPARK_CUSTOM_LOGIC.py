from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import *
from pyspark.sql.window import *


print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

Kist1=[]
#
# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()
print("Ingesting the customers csv file")
cust_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customers.csv')
print("**************")
print(cust_from_df.columns)
Kist1.append(list(cust_from_df.columns))
print("Ingesting the products csv file")
products_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\products.csv')
print("**************")
print(products_from_df.columns)
Kist1.append(list(products_from_df.columns))
print("Ingesting the orders csv file")
orders_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\orders.csv')
print("**************")
print(orders_from_df.columns)
Kist1.append(list(orders_from_df.columns))

print("Ingesting the sales csv file")
sales_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\sales.csv')
print("**************")
print(sales_from_df.columns)
Kist1.append(list(sales_from_df.columns))


str1="Coder"
print(f"Address of string {id(str1)}")
print(f"Address of string {id(str1.upper())}")
str1=str1.upper()
print(f"Address of string {id(str1)}")
print(str1)

list1=[1,2,3,4]
print(f"Address of list {id(list)}")
list1=list1.append(10)
print(f"List is here {list1} and address of list{id(list1)}")

print("""
Moral of the story-->The string are immutable then how come we can modify then 
actually when we are modifying it is creating new objects of the string and therefore its is allowing it to be modified 
same variables can point to different objects
""")

print("Dealing with custom logic in pyspark")
print("How to create a new column in dataframe")
products_from_df.select(products_from_df["product_ID"],products_from_df["quantity"].alias("quantity")).withColumn("New_quantitty",col("quantity").cast("Double")*1.25).show(6)
print("********")

products_from_df.printSchema()
print("How to create a new column in dataframe AND ADDING IT IN DATAFRAME")
products_from_df=products_from_df.select(products_from_df["product_ID"],products_from_df["quantity"].alias("quantity")).withColumn("New_quantitty",col("quantity").cast("Double")*1.25)
print("********")
products_from_df.printSchema()

print("How to update a existing column with a condition based logic in same dataframe")
products_from_df=products_from_df.withColumn("quantity",col("quantity")*100)
products_from_df.select(products_from_df["product_ID"],products_from_df["quantity"].alias("new_quantity")).show(6)

print("********")
print("Dealing with custom logic in pyspark-At function level")

@udf(returnType=StringType())
def custom_logic_func1(column_value):
   if column_value<18:
    return "Minor"
   if column_value>18 and column_value<60: 
    return "Adult"
   if column_value>60: 
    return"Elder"

@udf(returnType=StringType())
def custom_logic_func2(column_value1,column_value2):
   str_var=str(column_value2) 
   zip_code_sum_check=0
   for i in range(0,len(str_var)):
      
      if i==3:
       break
       zip_code_sum_check=zip_code_sum_check+int(i)    
      
   if column_value1<18 and zip_code_sum_check<10:
    return "Minor_valid"
   if column_value1<18 and zip_code_sum_check>10:
    return "Minor_invalid"
   if (column_value1>18 and column_value1<60) and zip_code_sum_check<10: 
    return "Adult_valid"
   if (column_value1>18 and column_value1<60) and zip_code_sum_check>10: 
    return "Adult_invalid" 
   if column_value1>60 and zip_code_sum_check<10: 
    return"Elder_valid"
   if column_value1>60 and zip_code_sum_check>10: 
    return"Elder_invalid"

@udf(returnType=StringType())
def custom_logic_func3(column_value1,column_value2):
   str_var=str(column_value2) 
   zip_code_sum_check=0
   counter=0
   for i in str_var:
      
      if counter==3:
        break
        counter=counter+1
      zip_code_sum_check=zip_code_sum_check+int(i)    
      
   if column_value1<18 and zip_code_sum_check<10:
    return str(zip_code_sum_check)+"@Minor_valid"
   if column_value1<18 and zip_code_sum_check>10:
    return str(zip_code_sum_check)+"@Minor_invalid"
   if (column_value1>18 and column_value1<60) and zip_code_sum_check<10: 
    return str(zip_code_sum_check)+"@Adult_valid"
   if (column_value1>18 and column_value1<60) and zip_code_sum_check>10: 
    return str(zip_code_sum_check)+"@Adult_invalid" 
   if column_value1>60 and zip_code_sum_check<10: 
    return str(zip_code_sum_check)+"@Elder_valid"
   if column_value1>60 and zip_code_sum_check>10: 
    return str(zip_code_sum_check)+"@Elder_invalid"    


print("How to apply a custom_logic to a single or multiple columns in pyspark dataframe")
print("We can do this using  UDF functions")
print("********")
print("CURRENTLY SHOWING HOW TO WRITE CUSTOM LOGIC UDF WITH SELECT API")
print("Custom_logic-->for single column of dataframe")
print("PS classify age in a new column age_level as minor ,adult and elder ")
print(cust_from_df.select(cust_from_df['customer_id'],cust_from_df['age'],custom_logic_func1(cust_from_df['age']).alias("age_Level")).show(6))
print("Custom_logic-->for multiple columns of dataframe")
print("""
PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10: 
    return "Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10: 
    return "Adult_invalid" 
   if age>60 and zip_code_sum_check<10: 
    return"Elder_valid"
   if age>60 and zip_code_sum_check>10: 
    return"Elder_invalid"
However just return the classified value

""")
print(cust_from_df.select(cust_from_df['customer_id'],cust_from_df['age'],cust_from_df['zip_code'],custom_logic_func2(cust_from_df['age'],cust_from_df['zip_code']).alias("age_level")).show(10))
print("********")
print("""
PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "zip_code_sum_check+@Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "zip_code_sum_check+@Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10: 
    return "zip_code_sum_check+@Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10: 
    return "zip_code_sum_check+@Adult_invalid" 
   if age>60 and zip_code_sum_check<10: 
    return"zip_code_sum_check+@Elder_valid"
   if age>60 and zip_code_sum_check>10: 
    return"zip_code_sum_check+@Elder_invalid"
*************************
However just return the classified value ,HERE we also need to store the zip_code_check
*************************
""")
print(cust_from_df.select(cust_from_df['customer_id'],cust_from_df['age'],cust_from_df['zip_code'],split(custom_logic_func3(cust_from_df['age'],cust_from_df['zip_code']),"@").getItem(0).alias("zip_code_sum_check"),split(custom_logic_func3(cust_from_df['age'],cust_from_df['zip_code']),"@").getItem(1).alias("age_Level")).show(10))
print("********")
print("CURRENTLY SHOWING HOW TO WRITE CUSTOM LOGIC UDF USING WITHCOLUMN FUNCTION API")

print("Custom_logic-->for single column of dataframe")
print("PS classify age in a new column age_level as minor ,adult and elder ")
print(cust_from_df.select(cust_from_df['customer_id'],cust_from_df['age']).withColumn("age_Level",custom_logic_func1(cust_from_df['age'])).show(6))
print("Custom_logic-->for multiple columns of dataframe")

print("""
PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10: 
    return "Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10: 
    return "Adult_invalid" 
   if age>60 and zip_code_sum_check<10: 
    return"Elder_valid"
   if age>60 and zip_code_sum_check>10: 
    return"Elder_invalid"
However just return the classified value

""")
print(cust_from_df.select(cust_from_df['customer_id'],cust_from_df['age'],cust_from_df['zip_code']).withColumn("C_age_level",custom_logic_func2(cust_from_df['age'],cust_from_df['zip_code'])).show(10))

print("********")
print("""
PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "zip_code_sum_check+@Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "zip_code_sum_check+@Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10: 
    return "zip_code_sum_check+@Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10: 
    return "zip_code_sum_check+@Adult_invalid" 
   if age>60 and zip_code_sum_check<10: 
    return"zip_code_sum_check+@Elder_valid"
   if age>60 and zip_code_sum_check>10: 
    return"zip_code_sum_check+@Elder_invalid"
*************************
However just return the classified value ,HERE we also need to store the zip_code_check
*************************
""")
print(cust_from_df.select(cust_from_df['customer_id'],cust_from_df['age'],cust_from_df['zip_code']).withColumn("zip_code_sum_check",split(custom_logic_func3(cust_from_df['age'],cust_from_df['zip_code']),"@").getItem(0)).withColumn("age_Level",split(custom_logic_func3(cust_from_df['age'],cust_from_df['zip_code']),"@").getItem(1)).show(10))

print("\n***** END *******\n")

"""
OUTPUT

ANALYSIS/PYSPARK/PYSPARK_CUSTOM_LOGIC.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_CUSTOM_LOGIC.py  

**********
22/09/05 21:27:59 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Ingesting the customers csv file
**************
['customer_id', 'customer_name', 'gender', 'age', 'home_address', 'zip_code', 'city', 'state', 'country']
Ingesting the products csv file
**************
['product_ID', 'product_type', 'product_name', 'size', 'colour', 'price', 'quantity', 'description']
Ingesting the orders csv file
**************
['order_id', 'customer_id', 'payment', 'order_date', 'delivery_date']
Ingesting the sales csv file
**************
['sales_id', 'order_id', 'product_id', 'price_per_unit', 'quantity', 'total_price']
Address of string 2409691287152
Address of string 2410130758320
Address of string 2410130759600
CODER
Address of list 140721887802896
List is here None and address of list140721887807480

Moral of the story-->The string are immutable then how come we can modify then
actually when we are modifying it is creating new objects of the string and therefore its is allowing it to be modified
same variables can point to different objects

Dealing with custom logic in pyspark
How to create a new column in dataframe
+----------+--------+-------------+
|product_ID|quantity|New_quantitty|
+----------+--------+-------------+
|         0|      66|         82.5|
|         1|      53|        66.25|
|         2|      54|         67.5|
|         3|      69|        86.25|
|         4|      47|        58.75|
|         5|      45|        56.25|
+----------+--------+-------------+
only showing top 6 rows

********
root
 |-- product_ID: integer (nullable = true)
 |-- product_type: string (nullable = true)
 |-- product_name: string (nullable = true)
 |-- size: string (nullable = true)
 |-- colour: string (nullable = true)
 |-- price: integer (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- description: string (nullable = true)

How to create a new column in dataframe AND ADDING IT IN DATAFRAME
********
root
 |-- product_ID: integer (nullable = true)
 |-- quantity: integer (nullable = true)
 |-- New_quantitty: double (nullable = true)

How to update a existing column with a condition based logic in same dataframe
+----------+------------+
|product_ID|new_quantity|
+----------+------------+
|         0|        6600|
|         1|        5300|
|         2|        5400|
|         3|        6900|
|         4|        4700|
|         5|        4500|
+----------+------------+
only showing top 6 rows

********
Dealing with custom logic in pyspark-At function level
How to apply a custom_logic to a single or multiple columns in pyspark dataframe
We can do this using  UDF functions
********
CURRENTLY SHOWING HOW TO WRITE CUSTOM LOGIC UDF WITH SELECT API
Custom_logic-->for single column of dataframe
PS classify age in a new column age_level as minor ,adult and elder
+-----------+---+---------+
|customer_id|age|age_Level|
+-----------+---+---------+
|          1| 30|    Adult|
|          2| 69|    Elder|
|          3| 59|    Adult|
|          4| 67|    Elder|
|          5| 30|    Adult|
|          6| 40|    Adult|
+-----------+---+---------+
only showing top 6 rows

None
Custom_logic-->for multiple columns of dataframe

PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10:
    return "Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10:
    return "Adult_invalid"
   if age>60 and zip_code_sum_check<10:
    return"Elder_valid"
   if age>60 and zip_code_sum_check>10:
    return"Elder_invalid"
However just return the classified value


+-----------+---+--------+-----------+
|customer_id|age|zip_code|  age_level|
+-----------+---+--------+-----------+
|          1| 30|    5464|Adult_valid|
|          2| 69|    8223|Elder_valid|
|          3| 59|    5661|Adult_valid|
|          4| 67|    1729|Elder_valid|
|          5| 30|    4032|Adult_valid|
|          6| 40|    9996|Adult_valid|
|          7| 76|     793|Elder_valid|
|          8| 75|    7681|Elder_valid|
|          9| 51|       2|Adult_valid|
|         10| 70|    2118|Elder_valid|
+-----------+---+--------+-----------+
only showing top 10 rows

None
********

PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "zip_code_sum_check+@Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "zip_code_sum_check+@Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10:
    return "zip_code_sum_check+@Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10:
    return "zip_code_sum_check+@Adult_invalid"
   if age>60 and zip_code_sum_check<10:
    return"zip_code_sum_check+@Elder_valid"
   if age>60 and zip_code_sum_check>10:
    return"zip_code_sum_check+@Elder_invalid"
*************************
However just return the classified value ,HERE we also need to store the zip_code_check
*************************

+-----------+---+--------+------------------+-------------+
|customer_id|age|zip_code|zip_code_sum_check|    age_Level|
+-----------+---+--------+------------------+-------------+
|          1| 30|    5464|                19|Adult_invalid|
|          2| 69|    8223|                15|Elder_invalid|
|          3| 59|    5661|                18|Adult_invalid|
|          4| 67|    1729|                19|Elder_invalid|
|          5| 30|    4032|                 9|  Adult_valid|
|          6| 40|    9996|                33|Adult_invalid|
|          7| 76|     793|                19|Elder_invalid|
|          8| 75|    7681|                22|Elder_invalid|
|          9| 51|       2|                 2|  Adult_valid|
|         10| 70|    2118|                12|Elder_invalid|
+-----------+---+--------+------------------+-------------+
only showing top 10 rows

None
********
CURRENTLY SHOWING HOW TO WRITE CUSTOM LOGIC UDF USING WITHCOLUMN FUNCTION API
Custom_logic-->for single column of dataframe
PS classify age in a new column age_level as minor ,adult and elder
+-----------+---+---------+
|customer_id|age|age_Level|
+-----------+---+---------+
|          1| 30|    Adult|
|          2| 69|    Elder|
|          3| 59|    Adult|
|          4| 67|    Elder|
|          5| 30|    Adult|
|          6| 40|    Adult|
+-----------+---+---------+
only showing top 6 rows

None
Custom_logic-->for multiple columns of dataframe

PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10:
    return "Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10:
    return "Adult_invalid"
   if age>60 and zip_code_sum_check<10:
    return"Elder_valid"
   if age>60 and zip_code_sum_check>10:
    return"Elder_invalid"
However just return the classified value


+-----------+---+--------+-----------+
|customer_id|age|zip_code|C_age_level|
+-----------+---+--------+-----------+
|          1| 30|    5464|Adult_valid|
|          2| 69|    8223|Elder_valid|
|          3| 59|    5661|Adult_valid|
|          4| 67|    1729|Elder_valid|
|          5| 30|    4032|Adult_valid|
|          6| 40|    9996|Adult_valid|
|          7| 76|     793|Elder_valid|
|          8| 75|    7681|Elder_valid|
|          9| 51|       2|Adult_valid|
|         10| 70|    2118|Elder_valid|
+-----------+---+--------+-----------+
only showing top 10 rows

None
********

PS classify age in a new column age_level as minor ,adult and elder on also on the sum of first three characters of zip_code as zip_code_sum_check should be less then 10
   if age<18 and zip_code_sum_check<10:
    return "zip_code_sum_check+@Minor_valid"
   if age<18 and zip_code_sum_check>10:
    return "zip_code_sum_check+@Minor_invalid"
   if (age>18 and column_value1<60) and zip_code_sum_check<10:
    return "zip_code_sum_check+@Adult_valid"
   if (age>18 and column_value1<60) and zip_code_sum_check>10:
    return "zip_code_sum_check+@Adult_invalid"
   if age>60 and zip_code_sum_check<10:
    return"zip_code_sum_check+@Elder_valid"
   if age>60 and zip_code_sum_check>10:
    return"zip_code_sum_check+@Elder_invalid"
*************************
However just return the classified value ,HERE we also need to store the zip_code_check
*************************

+-----------+---+--------+------------------+-------------+
|customer_id|age|zip_code|zip_code_sum_check|    age_Level|
+-----------+---+--------+------------------+-------------+
|          1| 30|    5464|                19|Adult_invalid|
|          2| 69|    8223|                15|Elder_invalid|
|          3| 59|    5661|                18|Adult_invalid|
|          4| 67|    1729|                19|Elder_invalid|
|          5| 30|    4032|                 9|  Adult_valid|
|          6| 40|    9996|                33|Adult_invalid|
|          7| 76|     793|                19|Elder_invalid|
|          8| 75|    7681|                22|Elder_invalid|
|          9| 51|       2|                 2|  Adult_valid|
|         10| 70|    2118|                12|Elder_invalid|
+-----------+---+--------+------------------+-------------+
only showing top 10 rows

None

***** END *******

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 8800 (child process of PID 21400) has been terminated.

"""