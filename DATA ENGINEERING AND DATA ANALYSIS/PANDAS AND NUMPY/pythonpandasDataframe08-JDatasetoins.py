import numpy as np
import pandas as pd


sales=pd.read_csv('SHOPPING CHART DATA\sales.csv')
print(sales)
customers=pd.read_csv('SHOPPING CHART DATA\customers.csv')
print(customers)
orders=pd.read_csv('SHOPPING CHART DATA\orders.csv')
print(orders)
products=pd.read_csv('SHOPPING CHART DATA\products.csv')
print(products)

for l in [sales,customers,orders,products]:
   print(f" the dataframe  -->  has {l.shape} shape ")

   