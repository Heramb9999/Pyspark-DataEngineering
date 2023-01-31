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


print(f"Collection of dataset -->")
no1=int(1)
for i in Kist1:
    print(f" Schema {no1} is {i}")
    no1=no1+1
    
print("**************")
print("Different SQL statements PYSPARK OPERATIONS")

print("HOW TO USE SELECT AND WITHCOLUMN TOGETHER")
print("select distinct product_type, price,price*1.25 as NEW_PRICE_WITHGST from products")
products_from_df.select(products_from_df["product_type"],products_from_df["price"]).withColumn("NEW_PRICE_WITHGST",products_from_df["price"]*1.25).distinct().show(4)

print("**************")
print("select min(price),max(price),count(price),count(distinct price),count(colour),count(distinct colour) from data")
#print(products_from_df.select(min(products_from_df["price"]),max(products_from_df["price"]),count(products_from_df["price"]),countDistinct(products_from_df["price"]),count(products_from_df["colour"]),countDistinct(products_from_df["colour"])).show())
print(products_from_df.select(min(products_from_df["price"]),max(products_from_df["price"]),sum(products_from_df["price"]),count(products_from_df["colour"])).show())

print("select sum(price),colour from products group by colour")
print(products_from_df.groupBy("colour").sum("price").show())
print("**************")
print("select sum(price) as sumprice,avg(price) as avgprice,count(distinct product_name) as kitnaproducts,colour,product_type from products group by colour")
print(
products_from_df.groupBy([products_from_df["colour"],products_from_df["product_type"]]).agg(
    PSSQLFUNC.sum("price").alias("sumprice"),
    PSSQLFUNC.avg("price").alias("avgprice"),
    PSSQLFUNC.count("product_name").alias("kitnaproducts")
).show()
)

print("**************")
print("select sum(price) as sumprice,avg(price) as avgprice,count( product_name) as kitnaproducts,colour,product_type from products where group by colour='green' order by sumprice desc ")

print(
products_from_df.groupBy([products_from_df["colour"],products_from_df["product_type"]]).agg(
    PSSQLFUNC.sum("price").alias("sumprice"),
    PSSQLFUNC.avg("price").alias("avgprice"),
    PSSQLFUNC.count("product_name").alias("kitnaproducts")
).filter(products_from_df["colour"]=='green').sort(col('sumprice').desc()).show()
)

print("**************")
print("select sum(price) as sumprice,avg(price) as avgprice,count( product_name) as kitnaproducts,colour,product_type from products where group by colour='green' order by sumprice desc having avgprice>100 ")

print(
products_from_df.groupBy([products_from_df["colour"],products_from_df["product_type"]]).agg(
    PSSQLFUNC.sum("price").alias("sumprice"),
    PSSQLFUNC.avg("price").alias("avgprice"),
    PSSQLFUNC.count("product_name").alias("kitnaproducts")
).filter((products_from_df["colour"]=='green') & (col('avgprice')>105)).sort(col('sumprice').desc()).show()
)

print("**************")
print("PYSPARK Window partition by Crash course")

print(
    '''
    select product_id,product_type,price,colour,
    max(price) over(partition by colour order by product_type desc) as peak_price from data where product_type is not null   
    '''
 )

products_part=Window.partitionBy(products_from_df['colour']).orderBy(products_from_df['product_type'].desc())
print(f"Here observe the type of the created object(Window Spec object) --> {type(products_part)}")

#print(products_from_df.withColumn("peak_price1",max(products_from_df["price"]).over(products_part)).show(4))
products_from_df.select(products_from_df["product_ID"],products_from_df["product_type"],products_from_df["price"],products_from_df["colour"]).withColumn("peak_price",max(products_from_df["price"]).over(products_part)).filter(products_from_df["product_type"].isNotNull() ).show(4)


print(
    '''
    select product_id,product_type,price,colour,
    max(price) over(partition by colour order by product_type desc) as peak_price 
    ,min(price) over(partition by colour order by product_type desc) as low_price 
    ,row_number() over(partition by colour order by product_type desc) as r_no
    ,dense_rank() over(partition by colour order by product_type desc) as den_r_no
    from data where product_type is not null   
    '''
 )

products_from_df.select(products_from_df["product_ID"],products_from_df["product_type"],products_from_df["price"],products_from_df["colour"]).withColumn("peak_price",max(products_from_df["price"]).over(products_part)).withColumn("low_price",min(products_from_df["price"]).over(products_part)).withColumn("r_no",row_number().over(products_part)).withColumn("den_r_no",dense_rank().over(products_part)).filter(products_from_df["product_type"].isNotNull()).show(10)


print(
    '''
    select product_id,product_type,price,colour,
    max(price) over(partition by colour order by product_type desc) as peak_price 
    ,min(price) over(partition by colour order by product_type desc) as low_price 
    ,row_number() over(partition by colour order by product_type desc) as r_no
    ,dense_rank() over(partition by colour order by product_type desc) as den_r_no
    from data where product_type is not null  order by product_id desc 
    '''
 )

products_from_df.select(products_from_df["product_ID"],products_from_df["product_type"],products_from_df["price"],products_from_df["colour"]).withColumn("peak_price",max(products_from_df["price"]).over(products_part)).withColumn("low_price",min(products_from_df["price"]).over(products_part)).withColumn("r_no",row_number().over(products_part)).withColumn("den_r_no",dense_rank().over(products_part)).filter(products_from_df["product_type"].isNotNull()).sort(products_from_df['product_ID'].desc()).show(10)
print("*****end*******")

"""
OUTPUT

ANALYSIS/PYSPARK/PYSPARK_Dataframe_GROUPBY_02.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_Dataframe_GROUPBY_02.py  

**********
22/09/04 20:15:47 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
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
Collection of dataset -->
 Schema 1 is ['customer_id', 'customer_name', 'gender', 'age', 'home_address', 'zip_code', 'city', 'state', 'country']
 Schema 2 is ['product_ID', 'product_type', 'product_name', 'size', 'colour', 'price', 'quantity', 'description']
 Schema 3 is ['order_id', 'customer_id', 'payment', 'order_date', 'delivery_date']
 Schema 4 is ['sales_id', 'order_id', 'product_id', 'price_per_unit', 'quantity', 'total_price']
**************
Different SQL statements PYSPARK OPERATIONS
HOW TO USE SELECT AND WITHCOLUMN TOGETHER
select distinct product_type, price,price*1.25 as NEW_PRICE_WITHGST from products
+------------+-----+-----------------+
|product_type|price|NEW_PRICE_WITHGST|
+------------+-----+-----------------+
|       Shirt|   96|            120.0|
|    Trousers|   90|            112.5|
|       Shirt|  115|           143.75|
|    Trousers|   98|            122.5|
+------------+-----+-----------------+
only showing top 4 rows

**************
select min(price),max(price),count(price),count(distinct price),count(colour),count(distinct colour) from data
+----------+----------+----------+-------------+
|min(price)|max(price)|sum(price)|count(colour)|
+----------+----------+----------+-------------+
|        90|       119|    133315|         1260|
+----------+----------+----------+-------------+

None
select sum(price),colour from products group by colour
+------+----------+
|colour|sum(price)|
+------+----------+
|violet|     19045|
|orange|     19045|
| green|     19045|
|yellow|     19045|
|indigo|     19045|
|   red|     19045|
|  blue|     19045|
+------+----------+

None
**************
select sum(price) as sumprice,avg(price) as avgprice,count(distinct product_name) as kitnaproducts,colour,product_type from products group by colour
+------+------------+--------+------------------+-------------+
|colour|product_type|sumprice|          avgprice|kitnaproducts|
+------+------------+--------+------------------+-------------+
| green|      Jacket|    6445|107.41666666666667|           60|
|  blue|    Trousers|    6100|101.66666666666667|           60|
|   red|    Trousers|    6100|101.66666666666667|           60|
| green|    Trousers|    6100|101.66666666666667|           60|
|orange|       Shirt|    6500|108.33333333333333|           60|
|violet|    Trousers|    6100|101.66666666666667|           60|
|   red|       Shirt|    6500|108.33333333333333|           60|
|  blue|       Shirt|    6500|108.33333333333333|           60|
|  blue|      Jacket|    6445|107.41666666666667|           60|
|orange|    Trousers|    6100|101.66666666666667|           60|
|yellow|    Trousers|    6100|101.66666666666667|           60|
|indigo|    Trousers|    6100|101.66666666666667|           60|
|indigo|       Shirt|    6500|108.33333333333333|           60|
|yellow|      Jacket|    6445|107.41666666666667|           60|
|indigo|      Jacket|    6445|107.41666666666667|           60|
|   red|      Jacket|    6445|107.41666666666667|           60|
|orange|      Jacket|    6445|107.41666666666667|           60|
|yellow|       Shirt|    6500|108.33333333333333|           60|
| green|       Shirt|    6500|108.33333333333333|           60|
|violet|      Jacket|    6445|107.41666666666667|           60|
+------+------------+--------+------------------+-------------+
only showing top 20 rows

None
**************
select sum(price) as sumprice,avg(price) as avgprice,count( product_name) as kitnaproducts,colour,product_type from products where group by colour='green' order by sumprice desc
+------+------------+--------+------------------+-------------+
|colour|product_type|sumprice|          avgprice|kitnaproducts|
+------+------------+--------+------------------+-------------+
| green|       Shirt|    6500|108.33333333333333|           60|
| green|      Jacket|    6445|107.41666666666667|           60|
| green|    Trousers|    6100|101.66666666666667|           60|
+------+------------+--------+------------------+-------------+

None
**************
select sum(price) as sumprice,avg(price) as avgprice,count( product_name) as kitnaproducts,colour,product_type from products where group by colour='green' order by sumprice desc having avgprice>100    
+------+------------+--------+------------------+-------------+
|colour|product_type|sumprice|          avgprice|kitnaproducts|
+------+------------+--------+------------------+-------------+
| green|       Shirt|    6500|108.33333333333333|           60|
| green|      Jacket|    6445|107.41666666666667|           60|
+------+------------+--------+------------------+-------------+

None
**************
PYSPARK Window partition by Crash course

    select product_id,product_type,price,colour,
    max(price) over(partition by colour order by product_type desc) as peak_price from data where product_type is not null

Here observe the type of the created object(Window Spec object) --> <class 'pyspark.sql.window.WindowSpec'>
+----------+------------+-----+------+----------+
|product_ID|product_type|price|colour|peak_price|
+----------+------------+-----+------+----------+
|       870|    Trousers|  100|violet|       119|
|       871|    Trousers|  100|violet|       119|
|       872|    Trousers|  100|violet|       119|
|       873|    Trousers|  100|violet|       119|
+----------+------------+-----+------+----------+
only showing top 4 rows


    select product_id,product_type,price,colour,
    max(price) over(partition by colour order by product_type desc) as peak_price
    ,min(price) over(partition by colour order by product_type desc) as low_price
    ,row_number() over(partition by colour order by product_type desc) as r_no
    ,dense_rank() over(partition by colour order by product_type desc) as den_r_no
    from data where product_type is not null

+----------+------------+-----+------+----------+---------+----+--------+
|product_ID|product_type|price|colour|peak_price|low_price|r_no|den_r_no|
+----------+------------+-----+------+----------+---------+----+--------+
|       870|    Trousers|  100|violet|       119|       90|   1|       1|
|       871|    Trousers|  100|violet|       119|       90|   2|       1|
|       872|    Trousers|  100|violet|       119|       90|   3|       1|
|       873|    Trousers|  100|violet|       119|       90|   4|       1|
|       874|    Trousers|  100|violet|       119|       90|   5|       1|
|       905|    Trousers|  113|violet|       119|       90|   6|       1|
|       906|    Trousers|  113|violet|       119|       90|   7|       1|
|       907|    Trousers|  113|violet|       119|       90|   8|       1|
|       908|    Trousers|  113|violet|       119|       90|   9|       1|
|       909|    Trousers|  113|violet|       119|       90|  10|       1|
+----------+------------+-----+------+----------+---------+----+--------+
only showing top 10 rows


    select product_id,product_type,price,colour,
    max(price) over(partition by colour order by product_type desc) as peak_price
    ,min(price) over(partition by colour order by product_type desc) as low_price
    ,row_number() over(partition by colour order by product_type desc) as r_no
    ,dense_rank() over(partition by colour order by product_type desc) as den_r_no
    from data where product_type is not null  order by product_id desc

+----------+------------+-----+------+----------+---------+----+--------+
|product_ID|product_type|price|colour|peak_price|low_price|r_no|den_r_no|
+----------+------------+-----+------+----------+---------+----+--------+
|      1259|    Trousers|   91|violet|       119|       90|  60|       1|
|      1258|    Trousers|   91|violet|       119|       90|  59|       1|
|      1257|    Trousers|   91|violet|       119|       90|  58|       1|
|      1256|    Trousers|   91|violet|       119|       90|  57|       1|
|      1255|    Trousers|   91|violet|       119|       90|  56|       1|
|      1254|    Trousers|   91|indigo|       119|       90|  60|       1|
|      1253|    Trousers|   91|indigo|       119|       90|  59|       1|
|      1252|    Trousers|   91|indigo|       119|       90|  58|       1|
|      1251|    Trousers|   91|indigo|       119|       90|  57|       1|
|      1250|    Trousers|   91|indigo|       119|       90|  56|       1|
+----------+------------+-----+------+----------+---------+----+--------+
only showing top 10 rows

*****end*******
22/09/04 20:16:01 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 18944 (child process of PID 4192) has been terminated.
SUCCESS: The process with PID 4192 (child process of PID 11284) has been terminated.
"""