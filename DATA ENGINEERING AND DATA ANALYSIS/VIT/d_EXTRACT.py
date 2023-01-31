import pandas as pd
import numpy as np


sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')

dataframe_visit=pd.read_excel('DATA ENGINEERING AND DATA ANALYSIS\VIT\Calls Checks2 - Copy.xlsx',sheet_name='Salesforce Data Jan',header=1
,names=
['Store'
,'Status'
,'Non Productive Reason'
,'Call Type'
,'Resource'
,'Calls: Visit Code'
,'Original Date'
,'Visit Date'
,'Time In'
,'Time Out'
,'Duration'
]
)


print(dataframe_visit.head(15))
print(dataframe_visit[['Calls: Visit Code','Original Date']])
print(dataframe_visit[['Calls: Visit Code']])
#print(dataframe_visit.loc[0:10,['Calls: Visit Code','Original Date']])

'''
aggregateddata=transactiondata.groupby(by=["STORE_ID","household_key"],as_index=False).agg(
    sum_sales_value=('SALES_VALUE','sum'),
    mean_sales_value=('SALES_VALUE','median'),
    max_sales_value=('SALES_VALUE','max'),
    sum_quantity=('QUANTITY','sum'),
    mean_quantity=('QUANTITY','median'),
    max_quantity=('QUANTITY','max')
"user_id": pd.Series.nunique
aggregateddata=dataframe_visit.groupby(by=["Original Date","Resource"],as_index=False).agg(
distinct_calls=('Calls: Visit Code','nunique')
)
'''
aggregateddata_dd=dataframe_visit.groupby(by=["Original Date","Resource"],as_index=False).agg(
distinct_calls=(["Original Date","Resource"],'count')
)

aggregateddata01=dataframe_visit.groupby(by=["Original Date","Resource"],as_index=False)["Original Date","Resource"].count('Calls: Visit Code')
print('---------------')
print(aggregateddata01.head(10))
print('---------------*&&&&&&&&&&&&&&&&&&&&&&&&&&')
'''
print(aggregateddata01["distinct_calls"].value_counts())
'''

print(aggregateddata01.head(10))
