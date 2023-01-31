import pandas as pd
sales=pd.read_csv('SHOPPING CHART DATA\sales.csv')
print(sales.head(10))

customers=pd.read_csv('SHOPPING CHART DATA\customers.csv')
print(customers.head(10))
print(f"\n*********************************************\n")
print(customers[['customer_id','state']].head(10))
print(f"\n*********************************************\n")
print(customers.iloc[:,[0,3]])

print(f"\n*********************************************\n")
print(customers.iloc[:10,[0,3]])
print(f"\n*********************************************\n")
print(customers.iloc[:10,3:])
print(f"\n*********************************************\n")
print(customers.iloc[[1,5,10,15],4:])


print(f"\n*********************************************\n")
print(customers.loc[:10,['customer_id','state','gender']])
print(f"\n*********************************************\n")
#print(customers.loc[:10,[1,8,3]]) this is wrong will throw error

