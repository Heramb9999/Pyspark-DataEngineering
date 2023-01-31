import numpy as np
import pandas as pd
list2=[1,2,3,4,5,6]
numpyndarray=np.array(list2)
pdseries1=pd.Series(list2)
pdseries2=pd.Series(list2,index=['obj1','obj2','obj3','obj4','obj5','obj6'])

print("Ways to access the pandas series are as follows")
print(f"\n--- printing the pandas series with userdefined/custom index  -->\n {pdseries2} \n ")
print("Accessing Element from Series with Position")
print("Accessing Element -->")
print(pdseries2[:2])

print("Accessing Element -->")
print(pdseries2[-2:])
print("Accessing Element -->")
print(pdseries2[3])
print("Accessing Element -->")
print(pdseries2[1])


print(f"\n--- printing the pandas series with userdefined/custom index  -->\n {pdseries2} \n ")
print("Accessing Element -->")
print(pdseries2['obj4'])
print("Accessing Element -->")
print(pdseries2[['obj1','obj4']])

print("end -->")

listTRAV=[1,2,3,4,5,6,7,8]

print(f" LOC AND I LOC SERIES TRAVERSING")

pdseriesLOC=pd.Series(listTRAV,index=['obj1','obj2','obj3','obj4','obj5','obj6','obj7','obj8'])
print(f"\n--- printing the pandas series  listTRAV  -->\n {pdseriesLOC} \n ")
print("Accessing Element --> FROM INDEX POSITION")
print("Accessing Element -->")
print(pdseriesLOC.iloc[3])
print("Accessing Element -->")
print(pdseriesLOC.iloc[[1,3]])
print("Accessing Element -->")
print(pdseriesLOC.iloc[3:])
print("Accessing Element -->")
print(pdseriesLOC.iloc[:])


print("Accessing Element --> FROM LABELS /INDEX VALUES")

print("Accessing Element -->")
print(pdseriesLOC.loc['obj5'])
print("Accessing Element -->")
print(pdseriesLOC.loc[['obj3','obj7']])
print("Accessing Element -->")
print(pdseriesLOC.loc['obj5':])
print("Accessing Element -->")
print(pdseriesLOC.loc[:])
pdseriesLOC.index=['1obj1', '1obj2', '1obj3', '1obj4', '1obj5', '1obj6', '1obj7', '1obj8']
print(f"\n--- printing the pandas series  listTRAV  -->\n {pdseriesLOC} \n ")
