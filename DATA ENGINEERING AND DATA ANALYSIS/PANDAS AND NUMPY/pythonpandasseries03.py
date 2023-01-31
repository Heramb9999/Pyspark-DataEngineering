import numpy as np
import pandas as pd
'''
list2=[1,2,3,4,5,6]
pdseries1=pd.Series(list2)
pdseries2=pd.Series(list2,index=['obj1','obj2','obj3','obj4','obj5','obj6'])
listTRAV=[1,2,3,4,5,6,7,8]
pdseriesLOC=pd.Series(listTRAV,index=['obj1','obj2','obj3','obj4','obj5','obj6','obj7','obj8'])
print(" ------- pandas series---------\n",pdseriesLOC)

print(" ------- values  attribute of pandas series---------")
print(f"output-->\n{pdseriesLOC.values}\n")
print(f"output-->\n{type(pdseriesLOC.values)}\n")
print(" ------- index  attribute of pandas series---------")
print(f"output-->\n{pdseriesLOC.index}\n")
print(f"output-->\n{type(pdseriesLOC.index)}\n")
print(" ------- is_unique   attribute of pandas series---------")
print(f"output-->\n{pdseriesLOC.is_unique}\n")
print(f"output-->\n{type(pdseriesLOC.is_unique)}\n")
print(" ------- dtype   attribute of pandas series---------")
print(f"output-->\n{pdseriesLOC.dtype}\n")
print(f"output-->\n{type(pdseriesLOC.dtype)}\n")
print(" ------- size   attribute of pandas series---------")
print(f"output-->\n{pdseriesLOC.size}\n")
print(f"output-->\n{type(pdseriesLOC.size)}\n")
print(" ------- shape   attribute of pandas series---------")
print(f"output-->\n{pdseriesLOC.shape}\n")
print(f"output-->\n{type(pdseriesLOC.shape)}\n")
'''
listTRAV=[1,2,3,4,5,6,7,8]
pdseriesLOC=pd.Series(listTRAV,index=['obj1','obj2','obj3','obj4','obj5','obj6','obj7','obj8'])
print(" ------- pandas series---------\n",pdseriesLOC)
print(" ------- head and tail  method of pandas series---------")
print(f"output-->\n{pdseriesLOC.head()}\n")
print(" ------- head and tail  method of pandas series---------")
print(f"output-->\n{pdseriesLOC.tail()}\n")
print(f"output-->\n{pdseriesLOC.head(2)}\n")
print(f"output-->\n{pdseriesLOC.tail(2)}\n")

print(" ------- agg sum mean,etc  method of pandas series---------")
print(f"output-->\n {pdseriesLOC.sum()} \n")
print(f"output-->\n {pdseriesLOC.mean()} \n")
print(" ------- agg sum mean,etc  method of pandas series on selective data---------")
print(f"output-->\n {pdseriesLOC.iloc[:5].sum()} \n")
print(f"output-->\n {pdseriesLOC.iloc[:5].mean()} \n")

print(" ------- counting value method of pandas series---------")

listTRAVrepeative=[1,2,3,4,5,6,7,8,1,2,4,4,5,2,8,8,8,1,1,1,1,1]
indexlist=[]
for i in range(0,len(listTRAVrepeative)):
    temp="obj"+str(i)
    indexlist.append(temp)
pdseriesLOCrepeatative=pd.Series(listTRAVrepeative,index=[indexlist])
print(" ------- pandas series---------\n",pdseriesLOCrepeatative)
print(f"output-->\n {pdseriesLOCrepeatative.unique()} \n")
print(f"output-->\n {pdseriesLOCrepeatative.nunique()} \n")

print("important function")
print(f"output-->\n {pdseriesLOCrepeatative.value_counts()} \n")

print(" ------- sorting value  and index method of pandas series---------")

print(f"Sorting as per values output-->\n {pdseriesLOCrepeatative.sort_values(ascending=False)} \n")
print(f"Sorting as per values output-->\n {pdseriesLOCrepeatative.sort_values(ascending=False,inplace=False)} \n")

print(f"output-->\n {pdseriesLOCrepeatative.value_counts()} \n")

print(f" Sorting as per index output-->\n {pdseriesLOCrepeatative.value_counts().sort_index(ascending=False)} \n")
print(f" Sorting as per index output-->\n {pdseriesLOCrepeatative.value_counts().sort_index(ascending=False,inplace=False)} \n")

print(f" Sorting as per values output-->\n {pdseriesLOCrepeatative.value_counts().sort_values(ascending=False)} \n")
print(f" Sorting as per values output-->\n {pdseriesLOCrepeatative.value_counts().sort_values(ascending=False,inplace=False)} \n")

print(" ------- sorting value  and index method of pandas series---------")

sdata = pd.Series([1, 2, 3, np.nan, np.nan])
print(" ------- pandas series---------\n",sdata)
print(f"output-->\n {sdata.isna()} \n")
print(" -------  GET method of pandas series---------")
print(f"output-->\n {pdseriesLOCrepeatative.get('obj4',default='elementnotfound')} \n")
print(f"output-->\n {pdseriesLOCrepeatative.get(99999)} \n")
print(f"output-->\n {pdseriesLOCrepeatative.get(99999,default='elementnotfound')} \n")

print(" -------  CONVERT SERIES TO DATAFRAME method of pandas series---------")
df1=pdseriesLOCrepeatative.to_frame(name='measures')  

print(f"Dataframe output-->\n {df1} \n")

print(f"Pandas conditional filters")

print(pdseriesLOCrepeatative==1)

print(f"Pandas conditional filters getting only conditional filtered adata-->output")

print(pdseriesLOCrepeatative[pdseriesLOCrepeatative==1])

print(f"Pandas conditional filters getting only conditional filtered adata-->output")

print(pdseriesLOCrepeatative[[pdseriesLOCrepeatative<4 & pdseriesLOCrepeatative>6]])