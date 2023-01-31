from cmath import sqrt
from email import header
import pyspark
import pyspark.sql.types as pyspSQLTYPE
import pyspark.sql.functions as pyspSQLFunctions
from pyspark.sql import SparkSession



#
# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA'
'''
examples
sparkobj1.read.format("csv").option("header", True).option("header",True).options(inferSchema='True',delimiter=',')
.schema(schema).load("/tmp/resources/zipcodes.csv")

sparkobj1.read.format("csv").options(header= True,inferSchema='True',delimiter=',')
.schema(schema).load("/tmp/resources/zipcodes.csv")

'''

sparkobj1=SparkSession.builder.appName('Group by app').getOrCreate()

customers_df=sparkobj1.read.format('csv').options(header=True,inferSchema=True,delimiter=',')\
    .load("DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customers.csv")
customers_df.printSchema()
customers_df.show(10)

print(f"***********group by stuff**********")
print(f"**************************************")
print(f"\nCount the no of customers per city\n")
customers_df.groupBy('city').agg(
    pyspSQLFunctions.count("customer_name").alias("countofcustomers_percity")
).show(10)
print(f"\nCount the no of customers per city having count GREATER THAN 1\n")
customers_df.groupBy('city').agg(
    pyspSQLFunctions.count("customer_name").alias("countofcustomers_percity")
).where(pyspSQLFunctions.col('countofcustomers_percity')>1).show(10)
print(f"**************************************")
print(f"\n agg and find the  diff age statistics per city,gender wise \n")
customers_df.groupBy('city','gender').agg(
  pyspSQLFunctions.sum("age").alias('sum_age')
  ,pyspSQLFunctions.min("age").alias('min_age')
  ,pyspSQLFunctions.max("age").alias('max_age')
).show(10)


print(f"\n agg and find the  diff age statistics JUST gender wise \n")
customers_df.groupBy('gender').agg(
  pyspSQLFunctions.sum("age").alias('sum_age')
  ,pyspSQLFunctions.min("age").alias('min_age')
  ,pyspSQLFunctions.max("age").alias('max_age')
).show(10)

print(f"Implementing the same thing in SPARK SQL")
customers_df.createOrReplaceTempView('CUSTOMER_TABLE')
sparkobj1.sql("select sum(age) as sum_age,min(age) as min_age,max(age) as max_age,gender from CUSTOMER_TABLE group by gender ").show(10)

print(f"**************************************")
print(f" distinct  and count distinct")
print("EXAMPLE-02 USE CASE")
#SELECT COUNT(DISTINCT city),gender from customer group by gender 
print(f"SELECT COUNT(DISTINCT city),gender from customer group by gender")

customers_df.groupBy('gender').agg(
pyspSQLFunctions.count('city').alias('countcity')
,pyspSQLFunctions.count_distinct('city').alias('count_distinct_city')
).show(10)
#SELECT COUNT(DISTINCT city),gender from customer group by gender having COUNT(DISTINCT city)>1
print(f"SELECT COUNT(DISTINCT city),gender from customer group by gender having COUNT(DISTINCT city)>1 ")

customers_df.groupBy('gender').agg(
pyspSQLFunctions.count('city').alias('countcity')
,pyspSQLFunctions.count_distinct('city').alias('count_distinct_city')
).where(pyspSQLFunctions.col('count_distinct_city')>1).show(10)

print('********************end*********************')

'''
OUTPUT
22/03/11 02:09:43 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
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

***********group by stuff**********
**************************************

Count the no of customers per city

+---------------+------------------------+
|           city|countofcustomers_percity|
+---------------+------------------------+
|     Lake Lucas|                       1|
|    Sanfordberg|                       1|
|     Port Darcy|                       1|
| Hackettchester|                       1|
|Lake Lillyville|                       1|
|    Lake Joshua|                       1|
| South Zacmouth|                       1|
|  Dickinsonside|                       1|
|New Alexchester|                       1|
|     Chloeville|                       2|
+---------------+------------------------+
only showing top 10 rows


Count the no of customers per city having count GREATER THAN 1

+------------------+------------------------+
|              city|countofcustomers_percity|
+------------------+------------------------+
|        Chloeville|                       2|
|        Lake Jesse|                       2|
|          West Kai|                       2|
|          East Max|                       2|
|     Port Isabelle|                       2|
|     South Georgia|                       2|
|       East Sophia|                       3|
|    Schroedermouth|                       2|
|West Sebastianfort|                       2|
|           New Ava|                       3|
+------------------+------------------------+
only showing top 10 rows

**************************************

 agg and find the  diff age statistics per city,gender wise

+------------------+-----------+-------+-------+-------+
|              city|     gender|sum_age|min_age|max_age|
+------------------+-----------+-------+-------+-------+
|    Lake Jackmouth|     Female|     50|     50|     50|
|        West Henry|Genderqueer|     40|     40|     40|
|        Gracemouth| Non-binary|     23|     23|     23|
|       Harrismouth|    Agender|     27|     27|     27|
|     South Gabriel|    Agender|     80|     80|     80|
|       Clarkemouth|    Agender|     75|     75|     75|
|           Leebury| Non-binary|     55|     55|     55|
|New Madisonborough|       Male|     49|     49|     49|
|          New Ruby|       Male|     46|     46|     46|
|    New Callumtown|   Bigender|     75|     75|     75|
+------------------+-----------+-------+-------+-------+
only showing top 10 rows


 agg and find the  diff age statistics JUST gender wise

+-----------+-------+-------+-------+
|     gender|sum_age|min_age|max_age|
+-----------+-------+-------+-------+
|Genderqueer|   6348|     20|     80|
|    Agender|   5432|     20|     80|
|     Female|   5652|     20|     79|
| Polygender|   6364|     20|     79|
|   Bigender|   6123|     20|     80|
| Non-binary|   6783|     20|     80|
|       Male|   7326|     20|     80|
|Genderfluid|   5832|     20|     80|
+-----------+-------+-------+-------+

Implementing the same thing in SPARK SQL
+-------+-------+-------+-----------+
|sum_age|min_age|max_age|     gender|
+-------+-------+-------+-----------+
|   6348|     20|     80|Genderqueer|
|   5432|     20|     80|    Agender|
|   5652|     20|     79|     Female|
|   6364|     20|     79| Polygender|
|   6123|     20|     80|   Bigender|
|   6783|     20|     80| Non-binary|
|   7326|     20|     80|       Male|
|   5832|     20|     80|Genderfluid|
+-------+-------+-------+-----------+

**************************************
 distinct  and count distinct
EXAMPLE-02 USE CASE
SELECT COUNT(DISTINCT city),gender from customer group by gender

'''