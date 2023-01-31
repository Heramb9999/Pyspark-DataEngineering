import numpy as np
import pandas as pd

print("Creating  Dataframe from Dictionarys-->\n")
# Define a dictionary containing employee data
empdata = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':[27, 24, 22, 32],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
 
# Convert the dictionary into DataFrame 
dffromdict1 = pd.DataFrame(empdata)
print("the data from dataframe-->\n")
print(dffromdict1)

print("Column Selection:")
print(dffromdict1[['Age', 'Qualification']])
print("No of columns in df:")
print(f" lengthofcolumnsindfcalc  is {dffromdict1.iloc[:1,:].size}")
lengthofcolumnsindfcalc=int(dffromdict1.iloc[:1,:].size)-1
print("Insert Column in dataframe:")
print(dffromdict1.insert(lengthofcolumnsindfcalc,"new columnabc","345", False))
print("the data from dataframe-->\n")
print(dffromdict1)

print("Inserting Column anywhere:")
print(dffromdict1.insert(2,"pokemon",[1,"7982","jabra",888], False))
print("the data from dataframe-->\n")
print(dffromdict1)
print("Appending/inserting row in dataframe:")

dffromdict1=dffromdict1.append({'Name':'ahfihiuf','Age':676,'Address':'sgfug','Qualification':['iuaf8iyh','stgfutsf']},ignore_index ='True')
print("the data from dataframe-->\n")
print(dffromdict1)
print("creating a copy of df dffromdict1copy from dffromdict1 ")
dffromdict1copy=dffromdict1.copy()
dffromdict1=dffromdict1.append(dffromdict1copy,ignore_index ='True')
print("the data from dataframe-->\n")
print(dffromdict1)

empdatasap2 = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':[27, 24, 22, 32],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd'],
        'Qualification1':['Msc', 'MA', 'MCA', 'Phd']}
# Convert the dictionary into DataFrame 
dffromdict2 = pd.DataFrame(empdatasap2)
print("the data from dataframe-->\n")
print(dffromdict2)
print("DELETE OPERATION OF COLUMN /ROWS IN PANDAS DATAFRAME")
print("deleting Column  in dataframe:")

print("delete the column but inplace is false therefore no change in df -->\n  ",dffromdict2.drop(["Qualification1"],axis=1,inplace=False))
print("SEE displaying  from dataframe no change -->\n")
print(dffromdict2)
print("delete the column but inplace is true therefore is change in df -->\n  ",dffromdict2.drop(["Address","Qualification1"],axis=1,inplace=True))
print("SEE displaying  from dataframe Now  change have occurred column is dropped -->\n")
print(dffromdict2)

print("Deleting rows in dataframe:")

print("Deleting rows can be done in dataframe: by BOOLEAN MASK METHODS. BOOLEAN MASK/FILTERS ARE DONE TO FIND THE INDEX\n AND THE INDEX IS USED TO DELETE ROWS FROM DATAFRAME")
filter1=dffromdict2["Age"]>22
print("filter -->")
print(dffromdict2["Age"]>22)
print("Example 1==displaying the df after deleting rows in dataframe as a condition:")
print(dffromdict2.drop(index=dffromdict2[filter1].index,inplace=False))
filter2=dffromdict2["Name"].isin(['Jai','Kannauj','Gaurav'])
print("filter -->")
print(filter2)
print("Example 2==displaying the df after deleting rows in dataframe as a condition:@ in ")
print(dffromdict2.drop(index=dffromdict2[filter2].index,inplace=False))
filter3=~dffromdict2["Name"].isin(['Jai','Kannauj','Gaurav'])
print("filter -->")
print(filter3)
print("Example 3==displaying the df after deleting rows in dataframe as a condition: @not in")
print(dffromdict2.drop(index=dffromdict2[filter3].index,inplace=False))


