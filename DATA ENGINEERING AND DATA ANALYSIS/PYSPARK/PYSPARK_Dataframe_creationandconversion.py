from email import header
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

print(f"Default Spark Session object is [spark]")
spark=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
print(f"address of default spark object is {id(spark)} \n")
Sparkuserdefinedsession1=spark.newSession()
print(f"address of user defined spark object is {id(Sparkuserdefinedsession1)} \n")

columns = ["language","users_count"]
dataprog = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

empdatadict = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':['27', '24', '22', '32'],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}

print(f"\n************************************************\n")
print(f" PANDAS DATAFRAMES CREATED ")
print(pd.DataFrame(empdatadict))
panda_df=pd.DataFrame(empdatadict)

print(f"\n************************************************\n")
print(f" Pyspark DATAFRAMES CREATED ")
pysparkdf_01=spark.createDataFrame(data=dataprog,schema=columns)
pysparkdf_01.printSchema()


print(f"\n************************************************\n")
print(f"Converting Pandas dataframe to pyspark dataframe")
pysparkdf_from_pandasdf=spark.createDataFrame(panda_df)
print(type(pysparkdf_from_pandasdf))

print(f"\n************************************************\n")
print(f"Converting pyspark dataframe to Pandas dataframe-->   toPandas() function ")
pandasdf_created_from_pysparkdf=pysparkdf_01.toPandas()
print(type(pandasdf_created_from_pysparkdf))


print(f"\n************************************************\n")
print(f"Reading from Sources-->Importing Data")

csvpysparkdf=spark.read.options(inferSchema='True',delimiter=',').csv("DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customers.csv") 
print(csvpysparkdf.show())
csvpysparkdf_header=spark.read.options(inferSchema='True',delimiter=',',header='True').csv("DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SHOPPING CHART DATA\customers.csv") 
print(csvpysparkdf_header.show())



spark.stop()
Sparkuserdefinedsession1.stop()

'''
OUTPUT
G AND DATA ANALYSIS/PYSPARK/PYSPARK_Dataframe_creationandconversion.py"
Default Spark Session object is [spark]
22/01/23 15:19:07 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
address of default spark object is 2160390039696 

address of user defined spark object is 2160390040944 


************************************************

 PANDAS DATAFRAMES CREATED
     Name Age    Address Qualification
0     Jai  27      Delhi           Msc
1  Princi  24     Kanpur            MA
2  Gaurav  22  Allahabad           MCA
3    Anuj  32    Kannauj           Phd

************************************************

 Pyspark DATAFRAMES CREATED
root
 |-- language: string (nullable = true)
 |-- users_count: string (nullable = true)


************************************************

Converting Pandas dataframe to pyspark dataframe
<class 'pyspark.sql.dataframe.DataFrame'>

************************************************

Converting pyspark dataframe to Pandas dataframe-->   toPandas() function
<class 'pandas.core.frame.DataFrame'>

************************************************

Reading from Sources-->Importing Data
+-----------+-------------------+-----------+---+--------------------+--------+-------------------+--------------------+---------+
|        _c0|                _c1|        _c2|_c3|                 _c4|     _c5|                _c6|                 _c7|      _c8|
+-----------+-------------------+-----------+---+--------------------+--------+-------------------+--------------------+---------+
|customer_id|      customer_name|     gender|age|        home_address|zip_code|               city|               state|  country|
|          1|      Leanna Busson|     Female| 30|8606 Victoria Ter...|    5464|      Johnstonhaven|  Northern Territory|Australia|
|          2|Zabrina Harrowsmith|Genderfluid| 69|8327 Kirlin Summi...|    8223|    New Zacharyfort|     South Australia|Australia|
|          3|    Shina Dullaghan| Polygender| 59|269 Gemma SummitS...|    5661|           Aliburgh|Australian Capita...|Australia|
|          4|      Hewet McVitie|   Bigender| 67|743 Bailey GroveS...|    1729|  South Justinhaven|          Queensland|Australia|
|          5|     Rubia Ashleigh| Polygender| 30|48 Hyatt ManorSui...|    4032|     Griffithsshire|          Queensland|Australia|
|          6|     Cordey Tolcher|Genderfluid| 40|7118 Mccullough S...|    9996|         Blakehaven|     New South Wales|Australia|
|          7|    Winslow Ewbanck|   Bigender| 76|92 Hills Station ...|     793|          Masonfurt|          Queensland|Australia|
|          8|       Marlowe Wynn|    Agender| 75|383 Muller Summit...|    7681|            Samside|  Northern Territory|Australia|
|          9|  Brittaney Gontier|       Male| 51|57 Greenfelder Hi...|       2|          Beierport|  Northern Territory|Australia|
|         10|  Susanetta Wilshin|   Bigender| 70|615 Hayley KnollS...|    2118|          Joelburgh|   Western Australia|Australia|
|         11|Michaeline McIndrew|    Agender| 39|96 Daniel PlaceSu...|    7400|         Georgeland|  Northern Territory|Australia|
|         12|       Fedora Dmych|   Bigender| 78|66 Kayla MewsSuit...|    6334|        Taylorburgh|     South Australia|Australia|
|         13|    Marabel Swinfon|       Male| 42|16 Kuhn LoopSuite...|    6170|      Maddisonmouth|          Queensland|Australia|
|         14|    Chrissie Wackly|   Bigender| 36|403 Doherty RunSu...|    2050|North Benjaminville|     New South Wales|Australia|
|         15|     Avril Rossiter|Genderfluid| 34|254 Ali RidgeApt....|     491|         Kiehnburgh|   Western Australia|Australia|
|         16|  Gabbie Aldwinckle|       Male| 75|424 Mason PlaceAp...|    6438|            New Kai|            Victoria|Australia|
|         17|    Devonna Cutting|Genderfluid| 32|29 Imogen CrestSu...|    8309|     Lake Graceside|Australian Capita...|Australia|
|         18|      Chan Duchesne|       Male| 79|13 Bailey ManorAp...|    7171|         Walterland|     New South Wales|Australia|
|         19|   Chadwick Cruddas|    Agender| 41|002 Beau PlazaApt...|    7053|        West Bailey|            Victoria|Australia|
+-----------+-------------------+-----------+---+--------------------+--------+-------------------+--------------------+---------+
only showing top 20 rows

None
+-----------+-------------------+-----------+---+--------------------+--------+-------------------+--------------------+---------+
|customer_id|      customer_name|     gender|age|        home_address|zip_code|               city|               state|  country|
+-----------+-------------------+-----------+---+--------------------+--------+-------------------+--------------------+---------+
|          1|      Leanna Busson|     Female| 30|8606 Victoria Ter...|    5464|      Johnstonhaven|  Northern Territory|Australia|
|          2|Zabrina Harrowsmith|Genderfluid| 69|8327 Kirlin Summi...|    8223|    New Zacharyfort|     South Australia|Australia|
|          3|    Shina Dullaghan| Polygender| 59|269 Gemma SummitS...|    5661|           Aliburgh|Australian Capita...|Australia|
|          4|      Hewet McVitie|   Bigender| 67|743 Bailey GroveS...|    1729|  South Justinhaven|          Queensland|Australia|
|          5|     Rubia Ashleigh| Polygender| 30|48 Hyatt ManorSui...|    4032|     Griffithsshire|          Queensland|Australia|
|          6|     Cordey Tolcher|Genderfluid| 40|7118 Mccullough S...|    9996|         Blakehaven|     New South Wales|Australia|
|          7|    Winslow Ewbanck|   Bigender| 76|92 Hills Station ...|     793|          Masonfurt|          Queensland|Australia|
|          8|       Marlowe Wynn|    Agender| 75|383 Muller Summit...|    7681|            Samside|  Northern Territory|Australia|
|          9|  Brittaney Gontier|       Male| 51|57 Greenfelder Hi...|       2|          Beierport|  Northern Territory|Australia|
|         10|  Susanetta Wilshin|   Bigender| 70|615 Hayley KnollS...|    2118|          Joelburgh|   Western Australia|Australia|
|         11|Michaeline McIndrew|    Agender| 39|96 Daniel PlaceSu...|    7400|         Georgeland|  Northern Territory|Australia|
|         13|    Marabel Swinfon|       Male| 42|16 Kuhn LoopSuite...|    6170|      Maddisonmouth|          Queensland|Australia|
|         14|    Chrissie Wackly|   Bigender| 36|403 Doherty RunSu...|    2050|North Benjaminville|     New South Wales|Australia|
|         15|     Avril Rossiter|Genderfluid| 34|254 Ali RidgeApt....|     491|         Kiehnburgh|   Western Australia|Australia|
|         16|  Gabbie Aldwinckle|       Male| 75|424 Mason PlaceAp...|    6438|            New Kai|            Victoria|Australia|
|         17|    Devonna Cutting|Genderfluid| 32|29 Imogen CrestSu...|    8309|     Lake Graceside|Australian Capita...|Australia|
|         18|      Chan Duchesne|       Male| 79|13 Bailey ManorAp...|    7171|         Walterland|     New South Wales|Australia|
|         19|   Chadwick Cruddas|    Agender| 41|002 Beau PlazaApt...|    7053|        West Bailey|            Victoria|Australia|
|         20|       Gigi Kalaher|Genderqueer| 55|2069 Phoebe MallA...|    6318|        East Audrey|     South Australia|Australia|
+-----------+-------------------+-----------+---+--------------------+--------+-------------------+--------------------+---------+
only showing top 20 rows

None
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 24248 (child process of PID 2748) has been terminated.
SUCCESS: The process with PID 2748 (child process of PID 11296) has been terminated.
SUCCESS: The process with PID 11296 (child process of PID 24536) has been terminated.


'''