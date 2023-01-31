from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import concat,col


data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]
spark=SparkSession.builder.master("local[4]").appName('this is appname').getOrCreate()
schema = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df = spark.createDataFrame(data=data2,schema=schema)
print(df.rdd.zipWithIndex().collect())

df2=df.rdd.zipWithIndex().toDF()
df2.show()
