from tkinter import ON
#from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
import pyspark.sql as pySql
import pyspark.sql.functions as pysqlfunc


print(f"Default Spark Session object is [spark]")
spark=pySql.SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
Sparkuserdefinedsession1=spark.newSession()

print(f" PYSPARK COLUMN CLASS DEMO")

print(f"For the sale for practice we are creating Pyspark dataframes from Pandas dataframe")
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

print(f" *******************DEALING WITH PYSPARK COLUMN CLASS********************")


print("Selecting  specific columns from dataframe with general syntax")
py_df_employeeprofile.select(py_df_employeeprofile.Name).show()
py_df_employeeprofile.select(py_df_employeeprofile["Name"]).show()

py_df_employeeprofile.select(py_df_employeeprofile.Name,py_df_employeeprofile.Employeeid).show()
py_df_employeeprofile.select([py_df_employeeprofile["Name"],py_df_employeeprofile["Employeeid"]]).show()

print("\n Dealing and manipulating columns with COLUMN CLASS in pyspark\n")
#Using SQL col() function

py_df_employeeprofile.select(pysqlfunc.col('Name')).show()
print("This doesnot work-camnnot select multiple columns with column class")
#py_df_employeeprofile.select(pysqlfunc.col(['Name','Address','Employeeid'])).show()

print("How to give alias to a column in pyspark with .alias() function")

py_df_employeeprofile.select(py_df_employeeprofile["Name"].alias("apnaEmp")).show()
py_df_employeeprofile.select(pysqlfunc.concat(py_df_employeeprofile["Name"],py_df_employeeprofile["Age"]).alias("newcol")).show()

print(f"The cast() & astype()  functions are used to change the datatype of the column in pyspark")

py_df_employeeprofile.select(py_df_employeeprofile.Age,py_df_employeeprofile.Age.cast("int")).printSchema()

print(f" In operator in pyspark")
py_df_employeeprofile.select(py_df_employeeprofile.Name.isin('Anuj','Jai')).show()


spark.stop()
Sparkuserdefinedsession1.stop()

'''
OUTPUT OF THE PROGRAM
Default Spark Session object is [spark]
22/02/06 23:59:42 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
 PYSPARK COLUMN CLASS DEMO
For the sale for practice we are creating Pyspark dataframes from Pandas dataframe
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
 *******************DEALING WITH PYSPARK COLUMN CLASS********************
Selecting  specific columns from dataframe with general syntax
+-------+
|   Name|
+-------+
|    Jai|
| Princi|
| Gaurav|
|   Anuj|
|Sandesh|
+-------+

+-------+
|   Name|
+-------+
|    Jai|
| Princi|
| Gaurav|
|   Anuj|
|Sandesh|
+-------+

+-------+----------+
|   Name|Employeeid|
+-------+----------+
|    Jai|         1|
| Princi|         2|
| Gaurav|         3|
|   Anuj|         4|
|Sandesh|         5|
+-------+----------+

+-------+----------+
|   Name|Employeeid|
+-------+----------+
|    Jai|         1|
| Princi|         2|
| Gaurav|         3|
|   Anuj|         4|
|Sandesh|         5|
+-------+----------+


 Dealing and manipulating columns with COLUMN CLASS in pyspark

22/02/06 23:59:56 WARN ProcfsMetricsGetter: Exception when trying to compute pagesize, as a result reporting of ProcessTree metrics is stopped
+-------+
|   Name|
+-------+
|    Jai|
| Princi|
| Gaurav|
|   Anuj|
|Sandesh|
+-------+

This doesnot work-camnnot select multiple columns with column class
How to give alias to a column in pyspark with .alias() function
+-------+
|apnaEmp|
+-------+
|    Jai|
| Princi|
| Gaurav|
|   Anuj|
|Sandesh|
+-------+

+---------+
|   newcol|
+---------+
|    Jai27|
| Princi24|
| Gaurav22|
|   Anuj32|
|Sandesh29|
+---------+

The cast() & astype()  functions are used to change the datatype of the column in pyspark
root
 |-- Age: long (nullable = true)
 |-- Age: integer (nullable = true)

 In operator in pyspark
+---------------------+
|(Name IN (Anuj, Jai))|
+---------------------+
|                 true|
|                false|
|                false|
|                 true|
|                false|
+---------------------+



'''