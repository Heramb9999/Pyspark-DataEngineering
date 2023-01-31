import pandas as pd
import numpy as np


sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')

print(sales_panda_df.head(10))

sales_panda_df_grain=sales_panda_df['sales_id'].value_counts()
orders_panda_df_grain=orders_panda_df['customer_id'].value_counts()
orders_panda_df_grain2=orders_panda_df['order_id'].value_counts()

print(sales_panda_df_grain[sales_panda_df_grain.values>1])
print("the garin of orders id")
print(orders_panda_df_grain[orders_panda_df_grain.values>1])
print("the garin of orders id")
print(orders_panda_df_grain2[orders_panda_df_grain2.values>1])