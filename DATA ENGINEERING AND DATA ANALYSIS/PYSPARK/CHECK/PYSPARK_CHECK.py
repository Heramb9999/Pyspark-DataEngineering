#import findspark


import pyspark # only run after findspark.init()
#findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[3]').getOrCreate()

df = spark.sql('''select 'spark' as hello ''')
df.show()
spark.stop()
