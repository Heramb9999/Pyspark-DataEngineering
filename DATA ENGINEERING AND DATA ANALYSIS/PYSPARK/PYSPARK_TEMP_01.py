import pyspark
from pyspark.sql import SparkSession 
from pyspark.sql.types import StringType,StructField,StructType,IntegerType


spark=SparkSession.builder.appName('app01').getOrCreate()

sparkobj1=spark.newSession()
#sparkobj1.sparkContext.parallelize

list1=['1','2','3']
rdd1=sparkobj1.sparkContext.parallelize(list1)
print(rdd1.collect())
rdd1.getNumPartitions()
print(type(rdd1))
#rdd_df=rdd1.toDF('numbers')

schema01 = StructType([ \
    StructField("numbers",StringType(),True), \
  ])



rdd_df=sparkobj1.createDataFrame(rdd1,schema=schema01)
rdd_df.printSchema()
#rdd_df.rdd.getNumPartitions() 
'''
sparkobj1.read.format("csv").option("header", True).option("header",True).options(inferSchema='True',delimiter=',')
.schema(schema).load("/tmp/resources/zipcodes.csv")

sparkobj1.read.format("csv").options(header= True,inferSchema='True',delimiter=',')
.schema(schema).load("/tmp/resources/zipcodes.csv")

'''
data2 = [("James","","Smith","36636","M",3000),
    ("Michael","Rose","","40288","M",4000),
    ("Robert","","Williams","42114","M",4000),
    ("Maria","Anne","Jones","39192","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema02 = StructType([ \
    StructField("firstname",StringType(),True), \
    StructField("middlename",StringType(),True), \
    StructField("lastname",StringType(),True), \
    StructField("id", StringType(), True), \
    StructField("gender", StringType(), True), \
    StructField("salary", IntegerType(), True) \
  ])
 
df123 = spark.createDataFrame(data=data2,schema=schema02)
df123.printSchema()
df123.show(truncate=False)
df123.select(df123["firstname"]).show()
print(f"\n******************\n")

df123.createOrReplaceTempView("emp_table")
emp_op_df=spark.sql("select id from emp_table")
emp_op_df.show()
print(emp_op_df.count())

















spark.stop()
sparkobj1.stop()
print(f"stopped..")