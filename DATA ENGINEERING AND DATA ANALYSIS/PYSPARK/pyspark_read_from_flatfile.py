import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.types import StringType,StructField,StructType,IntegerType

spark=SparkSession.builder.appName('app01').getOrCreate()

sparkobj1=spark.newSession()

#
# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA'
'''
examples
sparkobj1.read.format("csv").option("header", True).option("header",True).options(inferSchema='True',delimiter=',')
.schema(schema).load("/tmp/resources/zipcodes.csv")

sparkobj1.read.format("csv").options(header= True,inferSchema='True',delimiter=',')
.schema(schema).load("/tmp/resources/zipcodes.csv")

'''

custom_schema=StructType(
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
print(f"\nThis is one way to ingest data from flat files in pyspark \n")
customersdf=sparkobj1.read.format("csv").options(header= True,inferSchema='True',delimiter=',').load("customers.csv")
print(f"*****************************************************")
print(customersdf.show(10))
print(f"*****************************************************")
print(f"\nThis is second way to ingest data from flat files in pyspark WITH CUSTOM SCHEMA \n")
customersdf_custom=sparkobj1.read.format("csv").schema(custom_schema).options(header= False,delimiter=',').load("customers.csv")
print(customersdf_custom.show(10))


print(f"*****************************************************")
print(f"\nThis is second way to ingest data from flat files in pyspark \n")
customers_read_csv_df_02=sparkobj1.read.options(header= True,inferSchema='True',delimiter=',').csv('customers.csv')
print(customers_read_csv_df_02.show(10))
print(f"*****************************************************")
print(f"\nThis is second way to ingest data from flat files in pyspark WITH CUSTOM SCHEMA \n")
customers_read_csv_df_02_custom=sparkobj1.read.schema(custom_schema).options(header= False,delimiter=',').csv('customers.csv')
print(customers_read_csv_df_02_custom.show(10))


sparkobj1.stop()

'''
OUTPUT OF THE CODE

D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PYSPARK/pyspark_read_from_flatfile.py"
22/03/11 00:05:29 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

This is one way to ingest data from flat files in pyspark

*****************************************************
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
*****************************************************

This is second way to ingest data from flat files in pyspark WITH CUSTOM SCHEMA

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
*****************************************************

This is second way to ingest data from flat files in pyspark

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
*****************************************************

This is second way to ingest data from flat files in pyspark WITH CUSTOM SCHEMA

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
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022\DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA> SUCCESS: The process with PID 12320 (child process of PID 23872) has been terminateed.


'''