import numpy as np
import pandas as pd

# list of strings
list1 = ['Geeks', 'For', 'Geeks', 'is', 'portal', 'for', 'Geeks']
 
# Calling DataFrame constructor on list
dflist = pd.DataFrame(list1)
print("Accessing the output-->\n")
print(dflist)
dflistcol = pd.DataFrame(list1,columns=['words'])
print("Accessing the output-->\n")
print("Displaying Dataframe from list-->\n")
print(dflistcol)
# Calling DataFrame constructor on list in a wrong way
empdatalist = [['Jai', 'Princi', 'Gaurav', 'Anuj'],
        ['27', '24', '22', '32'],
       ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        ['Msc', 'MA', 'MCA', 'Phd']]

print(empdatalist)
dflempdatalist101 = pd.DataFrame(empdatalist,columns=['name','age','city','education'])
print("Accessing the output-->\n")
print("Displaying Dataframe from list-->\n")
print(dflempdatalist101)


# Calling DataFrame constructor on list in a RIGHT way
empdatalist = [['Jai', 'Princi', 'Gaurav', 'Anuj'],
        ['27', '24', '22', '32'],
       ['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        ['Msc', 'MA', 'MCA', 'Phd']]
print(f"to correct things we are zipping the datastructures")
ZIPEDempdatalist=zip(empdatalist[0],empdatalist[1],empdatalist[2],empdatalist[3])
print(ZIPEDempdatalist)
dflempdatalist102 = pd.DataFrame(ZIPEDempdatalist,columns=['name','age','city','education'])
print("Accessing the output-->\n")
print("Displaying Dataframe from list which is zipped-->\n")
print(dflempdatalist102)



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
print("Accessing the output-->\n")
# select two columns
print(dffromdict1[['Name', 'Qualification']])
print("Accessing the output dataframe using iloc-->\n")
print(dffromdict1.iloc[1:,[0,2]])
print("Accessing the output dataframe using iloc-->\n")
print(dffromdict1.iloc[-2:,0:2])
print("Accessing the output dataframe using iloc-->\n")
print(dffromdict1.iloc[1:,2:])
print("Accessing the output dataframe using iloc-->\n")
print(dffromdict1.iloc[[0,2],2:])
print("Accessing the output dataframe using iloc full dataframe-->\n")
print(dffromdict1.iloc[:,:])


print("Accessing the output dataframe using loc i.e labels-->\n")
print(dffromdict1.loc[:,'Address'])
print("Accessing the output dataframe using loc i.e labels-->\n")
print(dffromdict1.loc[:,['Name','Address']])
print("Accessing the output dataframe using loc i.e labels-->\n")
print(dffromdict1.loc[1:,['Name','Address']])
print("Accessing single element  the output dataframe using loc i.e labels-->\n")
print(dffromdict1.loc[2,['Name','Address']])
print(" Accessing the output dataframe using loc i.e labels-->\n")
print(dffromdict1[['Name','Address','Qualification']])
