import collections
import numpy as np
import pandas as pd

print("Creating  Dataframe from Dictionarys-->\n")
# Define a dictionary containing employee data
empdata = {'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Age':[27, 24, 22, 32],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd']}
 
# Convert the dictionary into DataFrame 
dffromdict1sort = pd.DataFrame(empdata)

print(f"ORDER BY IN PANDAS ")
print(f"Sort by index IN PANDAS ")
print(f"The dataframe data is as follows \n")
print(dffromdict1sort.sort_index(axis=0,inplace=False,ascending=False))

print(f"Sort by Values/columns IN PANDAS ")
print(f"Sort by Values/columns-->single column IN PANDAS ")
print(f"The dataframe data is as follows \n")
print(dffromdict1sort.sort_values(["Age"],axis=0,inplace=False,ascending=False))
print(f"Sort by Values/columns-->Multiple  column IN PANDAS ")
print(f"The dataframe data is as follows \n")
print(dffromdict1sort.sort_values(["Name","Age"],axis=0,inplace=False,ascending=False))


