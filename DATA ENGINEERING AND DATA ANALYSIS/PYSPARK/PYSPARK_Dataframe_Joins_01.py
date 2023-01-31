from tkinter import ON
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import concat,col

print(f"Default Spark Session object is [spark]")
spark=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
Sparkuserdefinedsession1=spark.newSession()

print("Creating  Dataframe from Dictionarys-->\n")
# Define a dictionary containing Employee hierarchy
employeeprofile = {
        'Employeeid':['1', '2', '3', '4','5'],
        'Name':['Jai', 'Princi', 'Gaurav', 'Anuj','Sandesh'],
        'Age':[27, 24, 22, 32,29],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj','Nagpur'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd','Btech']}


employeepayroll = {
        'Employeeid':['1', '2', '3', '4'],
        'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Salary':[70000, 18000, 34000, 16800],
        'Managerid':['101', '105', '200', '200']
     }

Management = {
        'Managerid':['101', '102', '200', '205'],
        'Manager_Name':['Sndy', 'Reddemer', 'Scrin', 'Steelkunj'],
        'Manager_Salary':[170000, 118000, 234000, 316800],
        'level':['manager','manager','manager','manager'],
        'Division':['Sales', 'Analytics', 'Webdevelop', 'Salesforce']
     }

SeniorManagement = {
        'Managerid':['101', '102', '200', '205'],
        'Manager_Name':['Sndy', 'Reddemer', 'Scrin', 'Steelkunj'],
        'Manager_Salary':[170000, 118000, 234000, 316800],
        'level':['Seniormanager','Seniormanager','Seniormanager','Seniormanager'],
        'Division':['Sales', 'Analytics', 'Webdevelop', 'Salesforce']
     } 

print(f"\n************************************************\n")
print(f"-----Creating pandas dataframes------")

dfemployeeprofile=pd.DataFrame(employeeprofile)
print(f"\nThe dataframe data is as follows \n")
print(dfemployeeprofile)
dfemployeepayroll=pd.DataFrame(employeepayroll)
print(f"\nThe dataframe data is as follows \n")
print(dfemployeepayroll)
dfManagement=pd.DataFrame(Management)
print(f"\nThe dataframe data is as follows \n")
print(dfManagement)
dfSeniorManagement=pd.DataFrame(SeniorManagement)
print(f"\nThe dataframe data is as follows \n")
print(dfSeniorManagement)

print(f"\n************************************************\n")
print(f"-----Creating Pyspark dataframes------")
py_df_employeeprofile=spark.createDataFrame(data=dfemployeeprofile)
py_df_employeepayroll=spark.createDataFrame(data=dfemployeepayroll)
py_df_Management=spark.createDataFrame(data=dfManagement)
py_df_SeniorManagement=spark.createDataFrame(data=dfSeniorManagement)
print(f"\n************************************************\n")
print(py_df_employeeprofile.show())
print(f"\n************************************************\n")
print(py_df_employeepayroll.show())
print(f"\n************************************************\n")
print(py_df_Management.show())
print(f"\n************************************************\n")
print(py_df_SeniorManagement.show())

print(f"-----Creating Pyspark dataframes  JOINS------")

print(f"-----INNER JOIN------")
py_df_employeeprofile.join(other=py_df_employeepayroll,on=py_df_employeeprofile.Employeeid==py_df_employeepayroll.Employeeid,how='inner').show()

print(f"-----LEFT JOIN------")
py_df_employeeprofile.join(other=py_df_employeepayroll,on=py_df_employeeprofile.Employeeid==py_df_employeepayroll.Employeeid,how='left').show()

print(f"-----LEFT JOIN------")
py_df_employeepayroll.join(other=py_df_Management,on=py_df_employeepayroll.Managerid==py_df_Management.Managerid,how='left').show()


print(f"-----RIGHT JOIN------")
py_df_employeepayroll.join(other=py_df_Management,on=py_df_employeepayroll.Managerid==py_df_Management.Managerid,how='right').show()

print(f"-----full outer  JOIN------")
py_df_employeepayroll.join(other=py_df_Management,on=py_df_employeepayroll.Managerid==py_df_Management.Managerid,how='fullouter').show()
print(f"Note how a look on these type of joins Only columns from Left DATFRAME ARE FETCHED")
print(f"-----LEFT ANTI  JOIN------")
py_df_employeeprofile.join(other=py_df_employeepayroll,on=py_df_employeeprofile.Employeeid==py_df_employeepayroll.Employeeid,how='leftanti').show()

print(f"-----LEFT ANTI  JOIN------")
py_df_employeepayroll.join(other=py_df_Management,on=py_df_employeepayroll.Managerid==py_df_Management.Managerid,how='leftanti').show()

print(f"Note how a look on these type of joins Only columns from Left DATFRAME ARE FETCHED")
print(f"-----LEFT SEMI  JOIN------")
py_df_employeepayroll.join(other=py_df_Management,on=py_df_employeepayroll.Managerid==py_df_Management.Managerid,how='leftsemi').show()

spark.stop()
Sparkuserdefinedsession1.stop()

'''
OUTPUT 

To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Creating  Dataframe from Dictionarys-->


************************************************

-----Creating pandas dataframes------

The dataframe data is as follows

  Employeeid     Name  Age    Address Qualification
0          1      Jai   27      Delhi           Msc
1          2   Princi   24     Kanpur            MA
2          3   Gaurav   22  Allahabad           MCA
3          4     Anuj   32    Kannauj           Phd
4          5  Sandesh   29     Nagpur         Btech

The dataframe data is as follows

  Employeeid    Name  Salary Managerid
0          1     Jai   70000       101
1          2  Princi   18000       105
2          3  Gaurav   34000       200
3          4    Anuj   16800       200

The dataframe data is as follows

  Managerid Manager_Name  Manager_Salary    level    Division
0       101         Sndy          170000  manager       Sales
1       102     Reddemer          118000  manager   Analytics
2       200        Scrin          234000  manager  Webdevelop
3       205    Steelkunj          316800  manager  Salesforce

The dataframe data is as follows

  Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000  Seniormanager       Sales
1       102     Reddemer          118000  Seniormanager   Analytics
2       200        Scrin          234000  Seniormanager  Webdevelop
3       205    Steelkunj          316800  Seniormanager  Salesforce

************************************************

-----Creating Pyspark dataframes------

************************************************

+----------+-------+---+---------+-------------+
|Employeeid|   Name|Age|  Address|Qualification|
+----------+-------+---+---------+-------------+
|         1|    Jai| 27|    Delhi|          Msc|
|         2| Princi| 24|   Kanpur|           MA|
|         3| Gaurav| 22|Allahabad|          MCA|
|         4|   Anuj| 32|  Kannauj|          Phd|
|         5|Sandesh| 29|   Nagpur|        Btech|
+----------+-------+---+---------+-------------+

None

************************************************

+----------+------+------+---------+
|Employeeid|  Name|Salary|Managerid|
+----------+------+------+---------+
|         1|   Jai| 70000|      101|
|         2|Princi| 18000|      105|
|         3|Gaurav| 34000|      200|
|         4|  Anuj| 16800|      200|
+----------+------+------+---------+

None

************************************************

+---------+------------+--------------+-------+----------+
|Managerid|Manager_Name|Manager_Salary|  level|  Division|
+---------+------------+--------------+-------+----------+
|      101|        Sndy|        170000|manager|     Sales|
|      102|    Reddemer|        118000|manager| Analytics|
|      200|       Scrin|        234000|manager|Webdevelop|
|      205|   Steelkunj|        316800|manager|Salesforce|
+---------+------------+--------------+-------+----------+

None

************************************************

22/01/23 20:08:31 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
+---------+------------+--------------+-------------+----------+
|Managerid|Manager_Name|Manager_Salary|        level|  Division|
+---------+------------+--------------+-------------+----------+
|      101|        Sndy|        170000|Seniormanager|     Sales|
|      102|    Reddemer|        118000|Seniormanager| Analytics|
|      200|       Scrin|        234000|Seniormanager|Webdevelop|
|      205|   Steelkunj|        316800|Seniormanager|Salesforce|
+---------+------------+--------------+-------------+----------+

None
-----Creating Pyspark dataframes  JOINS------
-----INNER JOIN------
+----------+------+---+---------+-------------+----------+------+------+---------+
|Employeeid|  Name|Age|  Address|Qualification|Employeeid|  Name|Salary|Managerid|
+----------+------+---+---------+-------------+----------+------+------+---------+
|         3|Gaurav| 22|Allahabad|          MCA|         3|Gaurav| 34000|      200|
|         1|   Jai| 27|    Delhi|          Msc|         1|   Jai| 70000|      101|
|         4|  Anuj| 32|  Kannauj|          Phd|         4|  Anuj| 16800|      200|
|         2|Princi| 24|   Kanpur|           MA|         2|Princi| 18000|      105|
+----------+------+---+---------+-------------+----------+------+------+---------+

-----LEFT JOIN------
+----------+-------+---+---------+-------------+----------+------+------+---------+
|Employeeid|   Name|Age|  Address|Qualification|Employeeid|  Name|Salary|Managerid|
+----------+-------+---+---------+-------------+----------+------+------+---------+
|         3| Gaurav| 22|Allahabad|          MCA|         3|Gaurav| 34000|      200|
|         5|Sandesh| 29|   Nagpur|        Btech|      null|  null|  null|     null|
|         1|    Jai| 27|    Delhi|          Msc|         1|   Jai| 70000|      101|
|         4|   Anuj| 32|  Kannauj|          Phd|         4|  Anuj| 16800|      200|
|         2| Princi| 24|   Kanpur|           MA|         2|Princi| 18000|      105|
+----------+-------+---+---------+-------------+----------+------+------+---------+

-----LEFT JOIN------
+----------+------+------+---------+---------+------------+--------------+-------+----------+
|Employeeid|  Name|Salary|Managerid|Managerid|Manager_Name|Manager_Salary|  level|  Division|
+----------+------+------+---------+---------+------------+--------------+-------+----------+
|         3|Gaurav| 34000|      200|      200|       Scrin|        234000|manager|Webdevelop|
|         4|  Anuj| 16800|      200|      200|       Scrin|        234000|manager|Webdevelop|
|         1|   Jai| 70000|      101|      101|        Sndy|        170000|manager|     Sales|
|         2|Princi| 18000|      105|     null|        null|          null|   null|      null|
+----------+------+------+---------+---------+------------+--------------+-------+----------+

-----RIGHT JOIN------
+----------+------+------+---------+---------+------------+--------------+-------+----------+
|Employeeid|  Name|Salary|Managerid|Managerid|Manager_Name|Manager_Salary|  level|  Division|
+----------+------+------+---------+---------+------------+--------------+-------+----------+
|      null|  null|  null|     null|      205|   Steelkunj|        316800|manager|Salesforce|
|         3|Gaurav| 34000|      200|      200|       Scrin|        234000|manager|Webdevelop|
|         4|  Anuj| 16800|      200|      200|       Scrin|        234000|manager|Webdevelop|
|         1|   Jai| 70000|      101|      101|        Sndy|        170000|manager|     Sales|
|      null|  null|  null|     null|      102|    Reddemer|        118000|manager| Analytics|
+----------+------+------+---------+---------+------------+--------------+-------+----------+

-----full outer  JOIN------
+----------+------+------+---------+---------+------------+--------------+-------+----------+
|Employeeid|  Name|Salary|Managerid|Managerid|Manager_Name|Manager_Salary|  level|  Division|
+----------+------+------+---------+---------+------------+--------------+-------+----------+
|      null|  null|  null|     null|      205|   Steelkunj|        316800|manager|Salesforce|01
|         3|Gaurav| 34000|      200|      200|       Scrin|        234000|manager|Webdevelop|
|         4|  Anuj| 16800|      200|      200|       Scrin|        234000|manager|Webdevelop|
|         1|   Jai| 70000|      101|      101|        Sndy|        170000|manager|     Sales|
|      null|  null|  null|     null|      102|    Reddemer|        118000|manager| Analytics|
|         2|Princi| 18000|      105|     null|        null|          null|   null|      null|
+----------+------+------+---------+---------+------------+--------------+-------+----------+

Note how a look on these type of joins Only columns from Left DATFRAME ARE FETCHED
-----LEFT ANTI  JOIN------
+----------+-------+---+-------+-------------+
|Employeeid|   Name|Age|Address|Qualification|
+----------+-------+---+-------+-------------+
|         5|Sandesh| 29| Nagpur|        Btech|
+----------+-------+---+-------+-------------+

-----LEFT ANTI  JOIN------
+----------+------+------+---------+
|Employeeid|  Name|Salary|Managerid|
+----------+------+------+---------+
+----------+------+------+---------+

Note how a look on these type of joins Only columns from Left DATFRAME ARE FETCHED
-----LEFT SEMI  JOIN------
+----------+------+------+---------+
|Employeeid|  Name|Salary|Managerid|
+----------+------+------+---------+
|         3|Gaurav| 34000|      200|
|         4|  Anuj| 16800|      200|
|         1|   Jai| 70000|      101|
+----------+------+------+---------+

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 22780 (child process of PID 20732) has been terminated.
SUCCESS: The process with PID 20732 (child process of PID 776) has been terminated.
SUCCESS: The process with PID 776 (child process of PID 22008) has been terminated.

'''