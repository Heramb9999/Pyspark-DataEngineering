from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import when



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

print("Select orderid from orders")
print("**************")
print(orders_from_df.select(orders_from_df["order_id"]).show(4))
print("**************")
print(orders_from_df.select(orders_from_df.order_id).show(4))
print("******using sql  col function********")
print(orders_from_df.select(PSSQLFUNC.col('order_id')).show(4))

print("Select orderid as apnaid from orders")
print(orders_from_df.select(orders_from_df["order_id"].alias('apnaid')).show(4))

print("Select customer_name as apnanaam,gender as gg from customers")
print(cust_from_df.select(cust_from_df["customer_name"].alias('apnanaam'),cust_from_df['gender'].alias('gg')).show(4))
print("******using sql  col function********")
print(cust_from_df.select(PSSQLFUNC.col('customer_name').alias('apnanaam'),PSSQLFUNC.col('gender').alias('gg')).show(4))

print("Select customer_name as apnanaam,cast(age  as int) as umar from customers where gender='Female")
print("**************")
print(cust_from_df.select(cust_from_df["customer_name"].alias("apnanaam"),cust_from_df["Age"].astype("Int").alias("umar")).filter(cust_from_df['gender']=='Female').show(4))

print("For casting we can use .astype(datatype) or .cast(datatype)")
print("**************")
print("select distinct cast(age as float) as umar1,cast(age as int) as umar2,city from data where gender='female' limit 4 ")
print(cust_from_df.select(cust_from_df["Age"].cast("Float").alias("umar1"),cust_from_df["Age"].astype("Int").alias("umar2"),cust_from_df["city"]).filter(cust_from_df['gender']=='Female').distinct().show(4))

print("select distinct cast(age as float) as umar1,cast(age as int) as umar2 from data where gender='female' limit 4 ")
print("*******OPTION 1-.distinct() **************")
print(cust_from_df.select(cust_from_df["Age"].cast("Float").alias("umar1"),cust_from_df["Age"].astype("Int").alias("umar2")).filter(cust_from_df['gender']=='Female').distinct().show(4))
print("*******OPTION 2-.drop_duplicates() **************")
print(cust_from_df.select(cust_from_df["Age"].cast("Float").alias("umar1"),cust_from_df["Age"].astype("Int").alias("umar2"),cust_from_df["city"]).filter(cust_from_df['gender']=='Female').drop_duplicates(["umar1","umar2"]).show(4))

print("Select distinct  city from data where age between 18 and 25")
print(cust_from_df.select(cust_from_df["city"]).filter(cust_from_df["Age"].between(18,25)).distinct().show(4))
print("**************")
print(" df[col].isnull()  &  df[col].isnotnull() & df[col].like() ")

print("Select city,gender,customer_name from data where gender is notnull and customer_name like 'He%' ")
print("**************")
print(cust_from_df.select(cust_from_df["city"],cust_from_df["gender"],cust_from_df["customer_name"]).filter( (cust_from_df["gender"].isNotNull()) & (cust_from_df["customer_name"].like('He%'))))
print("**************")
print("select distinct city as base_City,substring(city,0,2) as city_code from data  ")

print(cust_from_df.select(cust_from_df["city"].alias("base_city"),cust_from_df["city"].substr(0,2).alias("city_code")).distinct().show(4))
print("**************")
print("select distinct gender,city from data where gender in ('Polygender','Bigender')")
print(cust_from_df.select(cust_from_df["gender"],cust_from_df["city"]).filter(cust_from_df["gender"].isin(['Polygender','Bigender'])).distinct().show(4))

print("**************")

cust_from_df.select(cust_from_df["gender"]).distinct().show()

print("Implementing case when in PYSPARK using When otherwise")
print(
"""
Select distinct gender,
case when gender='Genderqueer' then 'category1'
when gender='Agender' then 'category2'
when gender in ('Male','Female') then 'VALIDCATEGORY'
when gender='Polygender' then 'category4'
when gender='Bigender' then 'category5'
when gender='Non-binary' then 'category6'
when gender='Genderfluid' then 'category7'
else "Not defined" end as Gender_alloted_category
from data
"""       
 )
print("**************")
print(
cust_from_df.select(cust_from_df["gender"]
,when(cust_from_df["gender"]=='Genderqueer','category1').when(cust_from_df["gender"]=='Agender','category2')
.when(cust_from_df["gender"].isin(['Male','Female']),'VALIDCATEGORY').when(cust_from_df["gender"]=='Polygender','category4').when(cust_from_df["gender"]=='Bigender','category5')
.when(cust_from_df["gender"]=='Non-binary','category6').when(cust_from_df["gender"]=='Genderfluid','category7').otherwise("Not defined").alias("Gender_alloted_category")
).distinct().show()
)

print("REusing existing column with WITHCOLUMN FUNCTION")
print("**************")
print("select distinct customer_id,substring(customer_name,0,5) as customer_name ,age ,(100-age) as REM_AGE FROM data ")

#SELECT API AND WITHCOLUMN DOESNOT WORK TOGETHER AS PER THE BELOW EXAMPLE
#print(cust_from_df.select(cust_from_df["customer_id"],cust_from_df["customer_name"].substr(0,5),cust_from_df["age"],cust_from_df.withColumn('Rem_age',100-cust_from_df["age"])).distinct().show(10))


print(cust_from_df.select(cust_from_df["customer_id"],cust_from_df["customer_name"].substr(0,5),cust_from_df["age"],100-cust_from_df["age"].alias("rem_age")).distinct().show(4))

print("**************")
print("THE WITHCOLUMN FUNCTION IN PYSPARK DOESNOT ALTER THE SCHEMA OF EXISTING DATAFRAME")
print("CURRENT SCHEMA OF DATAFRAME")
cust_from_df.printSchema()
cust_from_df.withColumn("Premium_duration_years",100-PSSQLFUNC.col("age"))
print("AFTER WITHCOLUMN IS APPLIED --> SCHEMA OF DATAFRAME")
cust_from_df.printSchema()


print("Sorting in PYSPARK- By default it sorts in ASCENDING")
print("**************")
print("select customer_id,customer_name from data order by customer_id (asc) ")
print(cust_from_df.select(cust_from_df["customer_id"],cust_from_df["customer_name"]).sort(cust_from_df["customer_id"].asc()).show(4))
print("**************")
print("select customer_id,customer_name,age from data order by age desc ,customer_id (asc)")
print(cust_from_df.select(cust_from_df["customer_id"],cust_from_df["customer_name"],cust_from_df["age"]).sort(cust_from_df["age"].desc(),cust_from_df["customer_id"].asc()).show(4))

print("**************")
print("select customer_id,customer_name,age from data where gender='Female'  order by age desc ,customer_id (asc)")
print(cust_from_df.select(cust_from_df["customer_id"],cust_from_df["customer_name"],cust_from_df["age"]).filter(cust_from_df["gender"]=='Female').sort(cust_from_df["age"].desc(),cust_from_df["customer_id"].asc()).show(4))


print("\n **********END OF PROGRAM************** \n")


"""
output

ANALYSIS/PYSPARK/PYSPARK_Column_class_01.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_Column_class_01.py  

**********
22/09/02 13:56:43 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
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
Select orderid from orders
**************
+--------+
|order_id|
+--------+
|       1|
|       2|
|       3|
|       4|
+--------+
only showing top 4 rows

None
**************
+--------+
|order_id|
+--------+
|       1|
|       2|
|       3|
|       4|
+--------+
only showing top 4 rows

None
******using sql  col function********
+--------+
|order_id|
+--------+
|       1|
|       2|
|       3|
|       4|
+--------+
only showing top 4 rows

None
Select orderid as apnaid from orders
+------+
|apnaid|
+------+
|     1|
|     2|
|     3|
|     4|
+------+
only showing top 4 rows

None
Select customer_name as apnanaam,gender as gg from customers
+-------------------+-----------+
|           apnanaam|         gg|
+-------------------+-----------+
|      Leanna Busson|     Female|
|Zabrina Harrowsmith|Genderfluid|
|    Shina Dullaghan| Polygender|
|      Hewet McVitie|   Bigender|
+-------------------+-----------+
only showing top 4 rows

None
******using sql  col function********
+-------------------+-----------+
|           apnanaam|         gg|
+-------------------+-----------+
|      Leanna Busson|     Female|
|Zabrina Harrowsmith|Genderfluid|
|    Shina Dullaghan| Polygender|
|      Hewet McVitie|   Bigender|
+-------------------+-----------+
only showing top 4 rows

None
Select customer_name as apnanaam,cast(age  as int) as umar from customers where gender='Female
**************
+---------------+----+
|       apnanaam|umar|
+---------------+----+
|  Leanna Busson|  30|
|  Alair Grimwad|  32|
|Bernice Grindle|  38|
|Shanna Pietrzyk|  56|
+---------------+----+
only showing top 4 rows

None
For casting we can use .astype(datatype) or .cast(datatype)
**************
select distinct cast(age as float) as umar1,cast(age as int) as umar2,city from data where gender='female' limit 4
+-----+-----+-----------------+
|umar1|umar2|             city|
+-----+-----+-----------------+
| 38.0|   38|     Williammouth|
| 58.0|   58| South Ellieburgh|
| 54.0|   54|      Cameronport|
| 54.0|   54|West Victoriabury|
+-----+-----+-----------------+
only showing top 4 rows

None
select distinct cast(age as float) as umar1,cast(age as int) as umar2 from data where gender='female' limit 4
*******OPTION 1-.distinct() **************
+-----+-----+
|umar1|umar2|
+-----+-----+
| 22.0|   22|
| 30.0|   30|
| 47.0|   47|
| 55.0|   55|
+-----+-----+
only showing top 4 rows

None
*******OPTION 2-.drop_duplicates() **************
+-----+-----+-------------+
|umar1|umar2|         city|
+-----+-----+-------------+
| 22.0|   22|Jordanborough|
| 30.0|   30|Johnstonhaven|
| 47.0|   47| Lake William|
| 55.0|   55|   Justinport|
+-----+-----+-------------+
only showing top 4 rows

None
Select distinct  city from data where age between 18 and 25
+---------------+
|           city|
+---------------+
|     Port Darcy|
| Hackettchester|
|New Alexchester|
|    Clarkeburgh|
+---------------+
only showing top 4 rows

None
**************
 df[col].isnull()  &  df[col].isnotnull() & df[col].like()
Select city,gender,customer_name from data where gender is notnull and customer_name like 'He%'
**************
DataFrame[city: string, gender: string, customer_name: string]
**************
select distinct city as base_City,substring(city,0,2) as city_code from data
+---------------+---------+
|      base_city|city_code|
+---------------+---------+
|   Chelseahaven|       Ch|
|     Justinport|       Ju|
|    South Eliza|       So|
|West Danieltown|       We|
+---------------+---------+
only showing top 4 rows

None
**************
select distinct gender,city from data where gender in ('Polygender','Bigender')
+----------+-----------------+
|    gender|             city|
+----------+-----------------+
|Polygender|       Boyerville|
|Polygender|Lake Mitchellbury|
|Polygender|      Georgiatown|
|  Bigender|    Port Madeline|
+----------+-----------------+
only showing top 4 rows

None
**************
+-----------+
|     gender|
+-----------+
|Genderqueer|
|    Agender|
|     Female|
| Polygender|
|   Bigender|
| Non-binary|
|       Male|
|Genderfluid|
+-----------+

Implementing case when in PYSPARK using When otherwise

Select distinct gender,
case when gender='Genderqueer' then 'category1'
when gender='Agender' then 'category2'
when gender in ('Male','Female') then 'VALIDCATEGORY'
when gender='Polygender' then 'category4'
when gender='Bigender' then 'category5'
when gender='Non-binary' then 'category6'
when gender='Genderfluid' then 'category7'
else "Not defined" end as Gender_alloted_category
from data

**************
+-----------+-----------------------+
|     gender|Gender_alloted_category|
+-----------+-----------------------+
|     Female|          VALIDCATEGORY|
|Genderqueer|              category1|
|Genderfluid|              category7|
|   Bigender|              category5|
| Non-binary|              category6|
| Polygender|              category4|
|    Agender|              category2|
|       Male|          VALIDCATEGORY|
+-----------+-----------------------+

None
REusing existing column with WITHCOLUMN FUNCTION
**************
select distinct customer_id,substring(customer_name,0,5) as customer_name ,age ,(100-age) as REM_AGE FROM data
+-----------+------------------------------+---+------------------------+
|customer_id|substring(customer_name, 0, 5)|age|(100 - age AS `rem_age`)|
+-----------+------------------------------+---+------------------------+
|        201|                         Loni | 58|                      42|
|        462|                         Franc| 45|                      55|
|        623|                         Hort | 57|                      43|
|        686|                         Elber| 22|                      78|
+-----------+------------------------------+---+------------------------+
only showing top 4 rows

None
**************
THE WITHCOLUMN FUNCTION IN PYSPARK DOESNOT ALTER THE SCHEMA OF EXISTING DATAFRAME
CURRENT SCHEMA OF DATAFRAME
root
 |-- customer_id: integer (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- home_address: string (nullable = true)
 |-- zip_code: integer (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- country: string (nullable = true)

AFTER WITHCOLUMN IS APPLIED --> SCHEMA OF DATAFRAME
root
 |-- customer_id: integer (nullable = true)
 |-- customer_name: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- home_address: string (nullable = true)
 |-- zip_code: integer (nullable = true)
 |-- city: string (nullable = true)
 |-- state: string (nullable = true)
 |-- country: string (nullable = true)

Sorting in PYSPARK- By default it sorts in ASCENDING
**************
select customer_id,customer_name from data order by customer_id (asc)
+-----------+-------------------+
|customer_id|      customer_name|
+-----------+-------------------+
|          1|      Leanna Busson|
|          2|Zabrina Harrowsmith|
|          3|    Shina Dullaghan|
|          4|      Hewet McVitie|
+-----------+-------------------+
only showing top 4 rows

None
**************
select customer_id,customer_name,age from data order by age desc ,customer_id (asc)
+-----------+----------------+---+
|customer_id|   customer_name|age|
+-----------+----------------+---+
|         35|Adamo Saddington| 80|
|         39| Legra Neaverson| 80|
|        143| Hewitt Hanscome| 80|
|        155|     Myrle Kelby| 80|
+-----------+----------------+---+
only showing top 4 rows

None
**************
select customer_id,customer_name,age from data where gender='Female'  order by age desc ,customer_id (asc)
+-----------+---------------+---+
|customer_id|  customer_name|age|
+-----------+---------------+---+
|        152|Panchito Wybern| 79|
|        755|   Herbert Rock| 79|
|         98| Kenyon Lemerle| 78|
|        251| Jeanne Pringer| 78|
+-----------+---------------+---+
only showing top 4 rows

None

 **********END OF PROGRAM**************

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 13228 (child process of PID 21604) has been terminated.
"""