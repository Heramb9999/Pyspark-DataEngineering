import numpy as np
import pandas as pd


'''

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
   salesbyprod=pd.merge(sales,products,how='inner',suffixes=['_fact','_dim'],left_on='product_id',right_on='product_ID')
   
   salesbyprod.head()
   
   pd.pivot_table(salesbyprod,values='total_price',index='product_type',columns=['colour'])
'''

   
empdatadict = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':[27, 24, 22, 32],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
print(empdatadict.values)
#empdatadict_schema = list(empdatadict.keys)
#empdatadict_data = list(empdatadict.values)