import pyspark.sql.functions as PSSQLFUNC
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
import os
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import *


print("**********")
print(f"THE CURRENT PROGRAM EXECUTED IS  -->  {os.path.basename(__file__)}  \n")
print("**********")

# cd 'DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\JSON DATASETS'
sparkcustomobj=SparkSession.builder.master('local[4]').appName("This is app name").getOrCreate()

data_source = [("1","James","Smith","USA","CA"),
    ("2","Michael","Rose","USA","NY"),
    ("3","Robert","Williams","USA","CA"),
    ("4","Maria","Jones","USA","FL"),
    ("5","aiuhgiuafiu","","USA","FL"),
    ("6","Juicy","Bttaer","",""),
    ("12","James","Straford","UK","DK")
  ]
columns = ["emp_id","firstname","lastname","country","state"]

print("Simulating the source data")
df_source = sparkcustomobj.createDataFrame(data = data_source, schema = columns)
df_source.show(truncate=False)
"""
this doesnot work

print("Fixing the none na and null values")
df_source_NA_FIX=df_source.na.replace(to_replace="Calc_vc",value="",subset=["lastname","country","state"])
df_source_NA_FIX.show(truncate=False)
print("Tried the above replace fucntion it doesnpt work")
"""

print("Performing the cosmetic operations")
df_source_NA_FIX=df_source.withColumn("lastname", when(PSSQLFUNC.col("lastname")=="" ,"Calc_vc").otherwise(PSSQLFUNC.col("lastname"))) \
    .withColumn("country", when(PSSQLFUNC.col("country")=="" ,"Calc_vc").otherwise(PSSQLFUNC.col("country"))) \
        .withColumn("state", when(PSSQLFUNC.col("state")=="" ,"Calc_vc").otherwise(PSSQLFUNC.col("state"))) 
df_source_NA_FIX.show(truncate=False)

print("Creating the Hash key and Hash diff-SOURCE TABLE")
df_source_NA_FIX=df_source_NA_FIX.withColumn("Hash_key",PSSQLFUNC.sha2(df_source_NA_FIX["emp_id"],256))\
.withColumn("Hash_diff",PSSQLFUNC.sha2(PSSQLFUNC.concat(df_source_NA_FIX["firstname"],df_source_NA_FIX["lastname"],df_source_NA_FIX["country"],df_source_NA_FIX["state"]),256))
       
df_source_NA_FIX.show(truncate=False)

#print("Dumping the calculated output")
#df1.write.mode("append").format('txt').options(header=True,delimiter='|').save("\DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SCD_CDC\scd_dump1.txt")

#df1.write.csv("\DATA ENGINEERING AND DATA ANALYSIS\PYSPARK\SCD_CDC\scd_dump1.csv")

print("Simulating the Target data")
print("Creating a Target data to perform SCD operations")
data_target = [("1","James","Smith","USA","CA"),
    ("2","Michael","Rose","USA","NY"),
    ("3","Robert","Williams","Turkey","BLA"),
    ("4","Maria","Jones","USA","FL"),
    ("5","aiuhgiuafiu","","USA","FL"),
    ("6","Juicy","Bttaer","FRANCE","EE")
  ]
columns = ["emp_id","firstname","lastname","country","state"]





df_target = sparkcustomobj.createDataFrame(data = data_target, schema = columns)
print("Displaying the target data")
df_target.show(truncate=False)

print("Performing the cosmetic operations")
df_target_NA_FIX=df_target.withColumn("lastname", when(PSSQLFUNC.col("lastname")=="" ,"Calc_vc").otherwise(PSSQLFUNC.col("lastname"))) \
    .withColumn("country", when(PSSQLFUNC.col("country")=="" ,"Calc_vc").otherwise(PSSQLFUNC.col("country"))) \
        .withColumn("state", when(PSSQLFUNC.col("state")=="" ,"Calc_vc").otherwise(PSSQLFUNC.col("state"))) 
df_target_NA_FIX.show(truncate=False)

print("Creating the Hash key and Hash diff-TARGET TABLE")
df_target_NA_FIX=df_target_NA_FIX.withColumn("Hash_key",PSSQLFUNC.sha2(df_target_NA_FIX["emp_id"],256))\
.withColumn("Hash_diff",PSSQLFUNC.sha2(PSSQLFUNC.concat(df_target_NA_FIX["firstname"],df_target_NA_FIX["lastname"],df_target_NA_FIX["country"],df_target_NA_FIX["state"]),256))\
 .withColumn("c_timestamp",PSSQLFUNC.current_timestamp())
       
df_target_NA_FIX.show(truncate=False)

print("Creating tables from Dataframes")
df_source_NA_FIX.createTempView("Employee_source")

df_target_NA_FIX.createTempView("Employee_target")

print("Identifying the changed/updated records and dumping them in new Dataframe")
strvar1="""
select Employee_source.*,cast(current_timestamp as timestamp) as c_timestamp from Employee_source Employee_source inner join Employee_target Employee_target
on cast(Employee_source.Hash_key as varchar(500))=cast(Employee_target.Hash_key as varchar(500))
where Employee_source.Hash_diff<>Employee_target.Hash_diff
"""

updated_records_df=sparkcustomobj.sql(strvar1)
updated_records_df.show(truncate=False)

print("Identifying the NEW records and dumping them in new Dataframe")

strvar2="""
select Employee_source.*,cast(current_timestamp as timestamp) as c_timestamp  from Employee_source Employee_source left join Employee_target Employee_target
on cast(Employee_source.Hash_key as varchar(500))=cast(Employee_target.Hash_key as varchar(500))
where Employee_target.Hash_key is null
"""
new_records_df=sparkcustomobj.sql(strvar2)
new_records_df.show(truncate=False)


print("Identifying the UNCHANGED RECORDS  records from source and target and dumping them in new Dataframe")
strvar3="""
select  Employee_target.* from Employee_source Employee_source inner join Employee_target Employee_target
on cast(Employee_source.Hash_key as varchar(500))=cast(Employee_target.Hash_key as varchar(500))
and cast(Employee_source.Hash_diff as varchar(500))=cast(Employee_target.Hash_diff as varchar(500))

"""

unchanged_records_df=sparkcustomobj.sql(strvar3)
unchanged_records_df.show(truncate=False)




print("Displaying 3 cases")
print("****UPDATED RECORDS******")
updated_records_df.show(truncate=False)

print("****NEW RECORDS******")
new_records_df.show(truncate=False)

print("****UNCHANGED RECORDS******")
unchanged_records_df.show(truncate=False)

print("Clubbing the 3 cases")
df_final=unchanged_records_df.union(new_records_df).union(updated_records_df)
df_final.show(truncate=False)




