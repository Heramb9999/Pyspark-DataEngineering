from email import header
from statistics import mode
import pyspark as PS
from pyspark.sql import SparkSession 
from pyspark.sql.types import StringType,StructField,StructType,IntegerType
import os 

print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

#
# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

print("Reading Flatfile of format CSV")
print("Best way to do it from the all the available ways -->")

customer_custom_schema=StructType(
[
    StructField("customer_id_c",StringType(),True)
    , StructField("customer_name_c",StringType(),True)
     ,StructField("gender_c",StringType(),True)
      ,StructField("age_c",StringType(),True)
     ,StructField("home_address_c",StringType(),True)
     ,StructField("zip_code_c",StringType(),True)
      ,StructField("city_c",StringType(),True)
         ,StructField("state_c",StringType(),True)
            ,StructField("country_c",StringType(),True)
]
)



print(f"\nThis is one way to ingest data from flat files in pyspark-here we are providing custom schema to datafile \n")
custom_cust_from_df=sparkcustomobj.read.format('csv').options(delimiter=',').schema(customer_custom_schema).load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customers.csv')
print("**************")
print(custom_cust_from_df.show(10))
print("**************")
print(custom_cust_from_df.columns)

print(f"\nThis is one way to ingest data from flat files in pyspark-here we are inferring the data from the datafile \n")
cust_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customers.csv')
print("**************")
print(cust_from_df.show(10))
print("**************")
print(cust_from_df.columns)




print("Ingesting the products csv file")
products_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\products.csv')
print("**************")
print(products_from_df.columns)

print("Ingesting the orders csv file")
orders_from_df=sparkcustomobj.read.format('csv').options(header=True,inferSchema=True,delimiter=',').load('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\orders.csv')
print("**************")
print(orders_from_df.columns)


cust_orders=cust_from_df.join(other=orders_from_df,on=cust_from_df.customer_id==orders_from_df.customer_id,how='inner')
print("**************")
print(cust_orders.columns)

print("Writing the File -Using Pyspark Write -Joined data of Customers and Orders")

print("Now directly if i try to write the data we will get error-Found duplicate column(s) when inserting into file  -->customer_id is repeated")
#cust_orders.write.format("csv").options(header=True,delimiter=',',mode='overwrite').save('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customer_orders_analysis.csv')

print("In order to avoid such situations-How to avoid duplicate columns after join in PySpark ?")
cust_orders_new=cust_from_df.join(other=orders_from_df,on=cust_from_df.customer_id==orders_from_df.customer_id,how='inner').drop(orders_from_df.customer_id)
#cust_orders_new.write.format("csv").options(header=True,delimiter=',',mode='overwrite').save('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customer_orders_analysis.csv')
cust_orders_new.write.mode("overwrite").format("csv").options(header=True,delimiter=',').save('DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customer_orders_analysis.csv')

"""
OUTPUT

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> & C:/Users/AsusT/AppData/Local/Programs/Python/Python310/python.exe "d:/HERAMB/IMP D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PYSPARK/PYSPARK_read_and_write_from_flatfile_02.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_read_and_write_from_flatfile_02.py  

**********
22/08/26 17:10:58 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Reading Flatfile of format CSV
Best way to do it from the all the available ways -->

This is one way to ingest data from flat files in pyspark-here we are providing custom schema to datafile

**************
+-------------+-------------------+-----------+-----+--------------------+----------+-----------------+--------------------+---------+
|customer_id_c|    customer_name_c|   gender_c|age_c|      home_address_c|zip_code_c|           city_c|             state_c|country_c|
+-------------+-------------------+-----------+-----+--------------------+----------+-----------------+--------------------+---------+
|  customer_id|      customer_name|     gender|  age|        home_address|  zip_code|             city|               state|  country|
|            1|      Leanna Busson|     Female|   30|8606 Victoria Ter...|      5464|    Johnstonhaven|  Northern Territory|Australia|
|            2|Zabrina Harrowsmith|Genderfluid|   69|8327 Kirlin Summi...|      8223|  New Zacharyfort|     South Australia|Australia|
|            3|    Shina Dullaghan| Polygender|   59|269 Gemma SummitS...|      5661|         Aliburgh|Australian Capita...|Australia|
|            4|      Hewet McVitie|   Bigender|   67|743 Bailey GroveS...|      1729|South Justinhaven|          Queensland|Australia|
|            5|     Rubia Ashleigh| Polygender|   30|48 Hyatt ManorSui...|      4032|   Griffithsshire|          Queensland|Australia|
|            6|     Cordey Tolcher|Genderfluid|   40|7118 Mccullough S...|      9996|       Blakehaven|     New South Wales|Australia|
|            7|    Winslow Ewbanck|   Bigender|   76|92 Hills Station ...|       793|        Masonfurt|          Queensland|Australia|
|            8|       Marlowe Wynn|    Agender|   75|383 Muller Summit...|      7681|          Samside|  Northern Territory|Australia|
|            9|  Brittaney Gontier|       Male|   51|57 Greenfelder Hi...|         2|        Beierport|  Northern Territory|Australia|
+-------------+-------------------+-----------+-----+--------------------+----------+-----------------+--------------------+---------+
only showing top 10 rows

None
**************
['customer_id_c', 'customer_name_c', 'gender_c', 'age_c', 'home_address_c', 'zip_code_c', 'city_c', 'state_c', 'country_c']

This is one way to ingest data from flat files in pyspark-here we are inferring the data from the datafile

**************
+-----------+-------------------+-----------+---+--------------------+--------+-----------------+--------------------+---------+
|customer_id|      customer_name|     gender|age|        home_address|zip_code|             city|               state|  country|
+-----------+-------------------+-----------+---+--------------------+--------+-----------------+--------------------+---------+
|          1|      Leanna Busson|     Female| 30|8606 Victoria Ter...|    5464|    Johnstonhaven|  Northern Territory|Australia|
|          2|Zabrina Harrowsmith|Genderfluid| 69|8327 Kirlin Summi...|    8223|  New Zacharyfort|     South Australia|Australia|
|          3|    Shina Dullaghan| Polygender| 59|269 Gemma SummitS...|    5661|         Aliburgh|Australian Capita...|Australia|
|          4|      Hewet McVitie|   Bigender| 67|743 Bailey GroveS...|    1729|South Justinhaven|          Queensland|Australia|
|          5|     Rubia Ashleigh| Polygender| 30|48 Hyatt ManorSui...|    4032|   Griffithsshire|          Queensland|Australia|
|          6|     Cordey Tolcher|Genderfluid| 40|7118 Mccullough S...|    9996|       Blakehaven|     New South Wales|Australia|
|          7|    Winslow Ewbanck|   Bigender| 76|92 Hills Station ...|     793|        Masonfurt|          Queensland|Australia|
|          8|       Marlowe Wynn|    Agender| 75|383 Muller Summit...|    7681|          Samside|  Northern Territory|Australia|
|          9|  Brittaney Gontier|       Male| 51|57 Greenfelder Hi...|       2|        Beierport|  Northern Territory|Australia|
|         10|  Susanetta Wilshin|   Bigender| 70|615 Hayley KnollS...|    2118|        Joelburgh|   Western Australia|Australia|
+-----------+-------------------+-----------+---+--------------------+--------+-----------------+--------------------+---------+
only showing top 10 rows

None
**************
['customer_id', 'customer_name', 'gender', 'age', 'home_address', 'zip_code', 'city', 'state', 'country']
Ingesting the products csv file
**************
['product_ID', 'product_type', 'product_name', 'size', 'colour', 'price', 'quantity', 'description']
Ingesting the orders csv file
**************
['order_id', 'customer_id', 'payment', 'order_date', 'delivery_date']
**************
['customer_id', 'customer_name', 'gender', 'age', 'home_address', 'zip_code', 'city', 'state', 'country', 'order_id', 'customer_id', 'payment', 'order_date', 'delivery_date']
Writing the File -Using Pyspark Write -Joined data of Customers and Orders
Now directly if i try to write the data we will get error-Found duplicate column(s) when inserting into file  -->customer_id is repeated
In order to avoid such situations-How to avoid duplicate columns after join in PySpark ?
22/08/26 17:11:13 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped


"""