import numpy as np
import pandas as pd

#Creating Pandas series
list1=[1,'iaydi','85','adsad',55]
pandaser=print(pd.Series(list1))
print(f"\n--- printing the pandas series pandaser  -->\n {pd.Series(list1)} \n ")

list2=[1,2,3,4,5,6]
numpyndarray=np.array(list2)
pdseries1=pd.Series(list2)
pdseries2=pd.Series(list2,index=['obj1','obj2','obj3','obj4','obj5','obj6'])

pandaser1frommumpyarray=pd.Series(numpyndarray)
print(f"\n--- printing the pandas series 2 from numpy array -->\n {pd.Series(pandaser1frommumpyarray)} \n ")
print(f"\n--- printing the pandas series with default index  -->\n {pdseries1} \n ")
print(f"\n--- printing the pandas series with userdefined/custom index  -->\n {pdseries2} \n ")

print("Other method to change the index of the panda series")

print(f"\n CREATING PANDAS SERIES FROM DICT index  \n ")
print(f"\n CREATING PANDAS SERIES FROM DICT -THE KEY FROM DICT BECOMES INDEX VAL FROM DICT BECOME VALUES IN SERIES \n ")

pdserdict1=pd.Series({"a":"10","b":"20","c":"30"})
pdserdict2=pd.Series({"name":"gamer","salary":"20lakhs","age":"26"})
pdserdict3=pd.Series({"name":["gamer1","gamer2"],"salary":["20lakhs","40 lakhs"],"age":["26","36"]})

print(f"\n--- printing the pandas series from dictionarys pdserdict1  -->\n {pdserdict1} \n ")
print(f"\n--- printing the pandas series from dictionarys  pdserdict2-->\n {pdserdict2} \n ")
print(f"\n--- printing the pandas series from dictionarys  pdserdict3 -->\n {pdserdict3} \n ")
    

