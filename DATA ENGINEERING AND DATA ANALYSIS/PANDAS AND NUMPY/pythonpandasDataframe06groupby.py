import numpy as np
import pandas as pd

transactiondata=pd.read_csv('transaction_data.csv')
print(f"The dataframe data is as follows \n")
print(transactiondata.head(10))
print(f" HOW TO DO GROUP BY IN PYTHON PANDAS DATAFRAME")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print(f" GROUP BY single column ,with single aggregate column  with single operation ")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print("----------------group by query equivalent------------------")
#SELECT household_key,COUNT(PRODUCT_ID) FROM DATAFRAME GROUP BY household_key
print(transactiondata.groupby(by="household_key")['PRODUCT_ID'].count())
#SELECT STORE_ID,SUM(SALES_VALUE) FROM DATAFRAME GROUP BY STORE_ID
print(transactiondata.groupby(by=["STORE_ID"])["SALES_VALUE"].sum())
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print(f" GROUP BY MULTIPLE column ,with single aggregate column  with single operation ")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print("----------------group by query equivalent------------------")
#SELECT STORE_ID,household_key,SUM(SALES_VALUE) FROM DATAFRAME GROUP BY STORE_ID,household_key
print(transactiondata.groupby(by=["STORE_ID","household_key"])["SALES_VALUE"].sum())

print("----------------group by query equivalent------------------")
#SELECT STORE_ID,household_key,SUM(SALES_VALUE) FROM DATAFRAME GROUP BY STORE_ID,household_key
print(transactiondata.groupby(by=["STORE_ID","household_key"])["SALES_VALUE"].median())
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print(f" GROUP BY MULTIPLE column ,with MULTIPLE aggregate column  with single operation ")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
# WHILE DEALING WITH MULTIPLE AGGREGATE COLUMN IN ORDER TO DEL WITH INDEXES WE HAVE TO -->ELSE IT WILL THROW WARNING
# You need groupby with parameter as_index=False df = df.groupby(['id','product'])['quantity'].sum().reset_index()
# OR  add reset_index:df = df.groupby(['id','product'])['quantity'].sum().reset_index() 
#  
print("----------------group by query equivalent------------------")
#SELECT STORE_ID,household_key,SUM(SALES_VALUE),SUM(QUANTITY) FROM DATAFRAME GROUP BY STORE_ID,household_key
print(transactiondata.groupby(by=["STORE_ID","household_key"],as_index=False)["SALES_VALUE","QUANTITY"].sum())

print("----------------group by query equivalent------------------")
#SELECT STORE_ID,household_key,max(SALES_VALUE),max(QUANTITY) FROM DATAFRAME GROUP BY STORE_ID,household_key
print(transactiondata.groupby(by=["STORE_ID","household_key"],as_index=False)["SALES_VALUE","QUANTITY"].max())
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print(f" GROUP BY MULTIPLE column ,with MULTIPLE aggregate column  with MULTIPLE operation ")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print("----------------group by query equivalent------------------")
#SELECT STORE_ID,household_key,SUM(SALES_VALUE),SUM(QUANTITY),max(SALES_VALUE),max(QUANTITY) FROM DATAFRAME GROUP BY STORE_ID,household_key
print(transactiondata.groupby(by=["STORE_ID","household_key"],as_index=False)["SALES_VALUE","QUANTITY"].agg(['sum','median','max']))
print("-----------------------------------")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print(f" CORRECT WAY TO DO DIFFERENT AGGREAGTION ON DIFFERENT COLUMNS  WITH ALIAS WITH AGG FUNCTION AND NAMED AGGREGATED FUNCTION  ")
print(f" GROUP BY MULTIPLE column ,with MULTIPLE aggregate column  with MULTIPLE operation ")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print("-----------------------------------")
'''
SELECT STORE_ID,household_key,SUM(SALES_VALUE) AS sum_sales_value,median(SALES_VALUE) as mean_sales_value,max(SALES_VALUE) as max_sales_value
,SUM(QUANTITY) as sum_quantity,median(QUANTITY) as mean_quantity,max(QUANTITY)  as max_quantity
FROM DATAFRAME GROUP BY STORE_ID,household_key
'''

aggregateddata=transactiondata.groupby(by=["STORE_ID","household_key"],as_index=False).agg(
    sum_sales_value=('SALES_VALUE','sum'),
    mean_sales_value=('SALES_VALUE','median'),
    max_sales_value=('SALES_VALUE','max'),
    sum_quantity=('QUANTITY','sum'),
    mean_quantity=('QUANTITY','median'),
    max_quantity=('QUANTITY','max')
)

print("Printing the dataframe with multiple aggregates across multiple columns with alias \n")
print(aggregateddata)
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
print("-------------GROUP BY  FOLLOWED BY ORDER BY----------------------")
print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
#SELECT STORE_ID,household_key,SUM(SALES_VALUE) FROM DATAFRAME GROUP BY STORE_ID,household_key order by STORE_ID desc
print(transactiondata.groupby(by=["STORE_ID","household_key"])["SALES_VALUE"].sum().to_frame().sort_values(by='STORE_ID',ascending=False))

#SELECT STORE_ID,household_key,SUM(SALES_VALUE) FROM DATAFRAME GROUP BY STORE_ID,household_key order by SUM(SALES_VALUE) desc
print(transactiondata.groupby(by=["STORE_ID","household_key"])["SALES_VALUE"].sum().to_frame().sort_values(by='SALES_VALUE',ascending=False))

print("--------------------end--------------------------")

