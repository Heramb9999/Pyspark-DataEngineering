import pyspark # only run after findspark.init()
#findspark.init()
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
import os


print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

spark = SparkSession.builder.master('local[3]').getOrCreate()

empdatadict = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':['27', '24', '22', '32'],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}


empdatalist = [['Jai', 'Princi', 'Gaurav', 'Anuj'],
        ['27', '24', '22', '32'],
       ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        ['Msc', 'MA', 'MCA', 'Phd']]

print(f"\n************************************************\n")
print(f" PANDAS DATAFRAMES CREATED ")
print(pd.DataFrame(empdatadict))


print(f"Creating Spark dataframe from PANDAS DATAFRAME \n")
pandasdf01=pd.DataFrame(empdatadict)
pysparkdf_from_pandasdf=spark.createDataFrame(pandasdf01)
print(f"\n************************************************\n")
print(pysparkdf_from_pandasdf)
print(f"\n************************************************\n")
print(pysparkdf_from_pandasdf.show())

print("Observe an additional NONE is printed")
print(f"Different WAYS TO SHOW DATA")

"""
show() method prints weird None after table

"""
print(f"print whole data")
print(pysparkdf_from_pandasdf.collect())
print("***********************")
print(f"Print N number of rows")
print(pysparkdf_from_pandasdf.take(1))

print(f"Print FIRST 20 OR N number of rows- Using .show()  show can be used without Print ")
print("*****20 rows ******")
print(pysparkdf_from_pandasdf.show())
print("*****n rows ******")
print(pysparkdf_from_pandasdf.show(1))
print("*****.show function without print will not return additional NOne at end******")
pysparkdf_from_pandasdf.show()

print("END-->These are different displaying options in Pyspark")

"""
ANALYSIS/PYSPARK/PYSPARK_Data_show_options.py"
**********
THE CURRENT PROGRAM EXECUTED IS  -->  PYSPARK_Data_show_options.py  

**********
22/09/01 16:51:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

************************************************

 PANDAS DATAFRAMES CREATED
     Name Age    Address Qualification
0     Jai  27      Delhi           Msc
1  Princi  24     Kanpur            MA
2  Gaurav  22  Allahabad           MCA
3    Anuj  32    Kannauj           Phd
Creating Spark dataframe from PANDAS DATAFRAME


************************************************

DataFrame[Name: string, Age: string, Address: string, Qualification: string]

************************************************

+------+---+---------+-------------+
|  Name|Age|  Address|Qualification|
+------+---+---------+-------------+
|   Jai| 27|    Delhi|          Msc|
|Princi| 24|   Kanpur|           MA|
|Gaurav| 22|Allahabad|          MCA|
|  Anuj| 32|  Kannauj|          Phd|
+------+---+---------+-------------+

None
Observe an additional NONE is printed
Different WAYS TO SHOW DATA
print whole data
[Row(Name='Jai', Age='27', Address='Delhi', Qualification='Msc'), Row(Name='Princi', Age='24', Address='Kanpur', Qualification='MA'), Row(Name='Gaurav', Age='22', Address='Allahabad', Qualification='MCA'), Row(Name='Anuj', Age='32', Address='Kannauj', Qualification='Phd')]
***********************
Print N number of rows
[Row(Name='Jai', Age='27', Address='Delhi', Qualification='Msc')]
Print FIRST 20 OR N number of rows- Using .show()  show can be used without Print 
*****20 rows ******
+------+---+---------+-------------+
|  Name|Age|  Address|Qualification|
+------+---+---------+-------------+
|   Jai| 27|    Delhi|          Msc|
|Princi| 24|   Kanpur|           MA|
|Gaurav| 22|Allahabad|          MCA|
|  Anuj| 32|  Kannauj|          Phd|
+------+---+---------+-------------+

None
*****n rows ******
22/09/01 16:51:31 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
+----+---+-------+-------------+
|Name|Age|Address|Qualification|
+----+---+-------+-------------+
| Jai| 27|  Delhi|          Msc|
+----+---+-------+-------------+
only showing top 1 row

None
*****.show function without print will not return additional NOne at end******
+------+---+---------+-------------+
|  Name|Age|  Address|Qualification|
+------+---+---------+-------------+
|   Jai| 27|    Delhi|          Msc|
|Princi| 24|   Kanpur|           MA|
|Gaurav| 22|Allahabad|          MCA|
|  Anuj| 32|  Kannauj|          Phd|
+------+---+---------+-------------+

END-->These are different displaying options in Pyspark
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 22432 (child process of PID 8712) has been terminated.

"""