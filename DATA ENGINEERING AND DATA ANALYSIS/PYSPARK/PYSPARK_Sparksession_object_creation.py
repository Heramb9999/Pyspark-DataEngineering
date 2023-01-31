import pyspark
import os
from pyspark.sql import SparkSession

print("*******************")
print(f"NAME OF CURRENT PROGRAM IS --> {os.path.basename(__file__) } ")
print("*******************")

print(f"Default Spark Session object is [spark]")
spark=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
print(f"address of default spark object is {id(spark)} \n")
Sparkuserdefinedsession1=spark.newSession()
print(f"address of user defined spark object is {id(Sparkuserdefinedsession1)} \n")

print("Stopping the spark objects")
spark.stop()
Sparkuserdefinedsession1.stop()

"""
IS/PYSPARK/PYSPARK_Sparksession_object_creation.py"
Default Spark Session object is [spark]
22/01/22 15:40:04 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
address of default spark object is 1639605817104 

address of user defined spark object is 1639605818352

Stopping the spark objects
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> SUCCESS: The process with PID 19732 (child process of PID 4076) has been terminated.
SUCCESS: The process with PID 4076 (child process of PID 8760) has been terminated.
SUCCESS: The process with PID 8760 (child process of PID 24492) has been terminated.

"""

