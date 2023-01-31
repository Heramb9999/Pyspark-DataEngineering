import numpy as np
import pandas as pd

print("Creating  Dataframe from Dictionarys-->\n")
# Define a dictionary containing Employee hierarchy
employeeprofile = {
        'Employeeid':['1', '2', '3', '4','5'],
        'Name':['Jai', 'Princi', 'Gaurav', 'Anuj','Sandesh'],
        'Age':[27, 24, 22, 32,29],
        'Address':['Delhi', 'Kanpur', 'Allahabad', 'Kannauj','Nagpur'],
        'Qualification':['Msc', 'MA', 'MCA', 'Phd','Btech']}


employeepayroll = {
        'Empid':['1', '2', '3', '4'],
        'Name':['Jai', 'Princi', 'Gaurav', 'Anuj'],
        'Salary':[70000, 18000, 34000, 16800],
        'Managerid':['101', '105', '200', '200']
     }

Management = {
        'Managerid':['101', '102', '200', '205'],
        'Manager_Name':['Sndy', 'Reddemer', 'Scrin', 'Steelkunj'],
        'Manager_Salary':[170000, 118000, 234000, 316800],
        'level':['manager','manager','manager','manager'],
        'Division':['Sales', 'Analytics', 'Webdevelop', 'Salesforce']
     }

SeniorManagement = {
        'Managerid':['101', '102', '200', '205'],
        'Manager_Name':['Sndy', 'Reddemer', 'Scrin', 'Steelkunj'],
        'Manager_Salary':[170000, 118000, 234000, 316800],
        'level':['Seniormanager','Seniormanager','Seniormanager','Seniormanager'],
        'Division':['Sales', 'Analytics', 'Webdevelop', 'Salesforce']
     }     

dfemployeeprofile=pd.DataFrame(employeeprofile)
print(f"\nThe dataframe data is as follows \n")
print(dfemployeeprofile)
dfemployeepayroll=pd.DataFrame(employeepayroll)
print(f"\nThe dataframe data is as follows \n")
print(dfemployeepayroll)
dfManagement=pd.DataFrame(Management)
print(f"\nThe dataframe data is as follows \n")
print(dfManagement)
dfSeniorManagement=pd.DataFrame(SeniorManagement)
print(f"\nThe dataframe data is as follows \n")
print(dfSeniorManagement)


print(f"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
print(f" Pandas dataframes CONCAT MERGE JOIN APPEND")
print(f"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

print(f"--------------- Pandas dataframes CONCAT---------------")
print(f"--------------- Pandas dataframes CONCAT vertically---------------")
concatdataframe1=pd.concat([dfManagement,dfSeniorManagement],axis=0)
print(f"\nThe dataframe data is as follows \n")
print(f"\nThe Index is not reset here observe--> \n")
print(concatdataframe1)
concatdataframe2=pd.concat([dfManagement,dfSeniorManagement],axis=0,ignore_index=True)
print(f"\nThe dataframe data is as follows \n")
print(f"\nThe Index is  reset Yes here observe--> \n")
print(concatdataframe2)
print(f"--------------- Pandas dataframes CONCAT Horizontally---------------")
concatdataframe3=pd.concat([dfManagement,dfSeniorManagement],axis=1)
print(f"\nThe dataframe data is as follows \n")
print(f"\nThe concat is done on index horizontally--> \n")
print(concatdataframe3)
concatdataframe4=pd.concat([dfemployeepayroll,dfManagement],axis=1,join="inner")
print(f"\nThe dataframe data is as follows \n")
print(f"\nThe concat is done on index horizontally  but with joins inside it but  joining on column name/key--> \n")
print(concatdataframe4)
print(f"--------------- Pandas dataframes APPEND---------------")
print(f"--------------- Pandas dataframes APPEND IS A SPECIAL CASE OF CONCAT---------------")
concatdataframeappend=dfManagement.append([dfSeniorManagement],ignore_index=True)
print(concatdataframeappend)

print(f"--------------- Pandas dataframes MERGE---------------")
print(f"\nThe dataframe data is as follows \n")
dfmergeinner=pd.merge(dfemployeepayroll,dfManagement)
print(dfmergeinner)

print(f"\nThe dataframe data is as follows \n")
dfmergeinner1=pd.merge(dfemployeepayroll,dfManagement,on='Managerid')
print(dfmergeinner1)

print(f"\nThe dataframe data is as follows-->joining with different column names \n")
dfmergeinner2=pd.merge(dfemployeeprofile,dfemployeepayroll,suffixes=['_left','_right'],left_on=['Employeeid'],right_on=['Empid'])
print(dfmergeinner2)

print(f"\nThe dataframe data is as follows with different column names and multiple columns \n")
dfmergeinner3=pd.merge(dfemployeeprofile,dfemployeepayroll,suffixes=['_left','_right'],left_on=['Employeeid','Name'],right_on=['Empid','Name'])
print(dfmergeinner3)

print(f"\nFULLOUTER JOIN The dataframe data is as follows with different column names and multiple columns \n")
dfmergefull=pd.merge(dfemployeeprofile,dfemployeepayroll,how='outer',suffixes=['_left','_right'],left_on=['Employeeid','Name'],right_on=['Empid','Name'])
print(dfmergefull)

print(f"\nLEFT OUTER JOIN The dataframe data is as follows with different column names and multiple columns \n")
dfmergeLEFT=pd.merge(dfemployeeprofile,dfemployeepayroll,how='left',suffixes=['_left','_right'],left_on=['Employeeid','Name'],right_on=['Empid','Name'])
print(dfmergeLEFT)

print(f"\nRIGHT OUTER JOIN The dataframe data is as follows with different column names and multiple columns \n")
dfmergeRIGHT=pd.merge(dfemployeeprofile,dfemployeepayroll,how='right',suffixes=['_left','_right'],left_on=['Employeeid','Name'],right_on=['Empid','Name'])
print(dfmergeRIGHT)

print(f"--------------- Pandas dataframes JOIN---------------")
print(f"--------------- Pandas dataframes JOIN IS A SPECIAL CASE OF MERGE---------------")
print(f"-- Pandas dataframes JOIN does not support left,right outer joins it supports INNER AND FULL OUTER JOIN--")
print(f"\nThe dataframe data is as follows \n")
dfjoin1=dfemployeepayroll.merge(dfManagement,how='inner',on='Managerid')
print(dfjoin1)
print("-----------end-------------------")

'''
OUTPUT OF THE PROGRAM
ECTS 2022/PYSPARK-DATA ENGINEERING/pythonpandasDataframe07Joinswithdict.py"
Creating  Dataframe from Dictionarys-->


The dataframe data is as follows 

  Employeeid     Name  Age    Address Qualification
0          1      Jai   27      Delhi           Msc
1          2   Princi   24     Kanpur            MA
2          3   Gaurav   22  Allahabad           MCA
3          4     Anuj   32    Kannauj           Phd
4          5  Sandesh   29     Nagpur         Btech

The dataframe data is as follows

  Empid    Name  Salary Managerid
0     1     Jai   70000       101
1     2  Princi   18000       105
2     3  Gaurav   34000       200
3     4    Anuj   16800       200

The dataframe data is as follows

  Managerid Manager_Name  Manager_Salary    level    Division
0       101         Sndy          170000  manager       Sales
1       102     Reddemer          118000  manager   Analytics
2       200        Scrin          234000  manager  Webdevelop
3       205    Steelkunj          316800  manager  Salesforce

The dataframe data is as follows

  Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000  Seniormanager       Sales
1       102     Reddemer          118000  Seniormanager   Analytics
2       200        Scrin          234000  Seniormanager  Webdevelop
3       205    Steelkunj          316800  Seniormanager  Salesforce
$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
 Pandas dataframes CONCAT MERGE JOIN APPEND
$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
--------------- Pandas dataframes CONCAT---------------
--------------- Pandas dataframes CONCAT vertically---------------

The dataframe data is as follows


The Index is not reset here observe-->

  Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000        manager       Sales
1       102     Reddemer          118000        manager   Analytics
2       200        Scrin          234000        manager  Webdevelop
3       205    Steelkunj          316800        manager  Salesforce
0       101         Sndy          170000  Seniormanager       Sales
1       102     Reddemer          118000  Seniormanager   Analytics
2       200        Scrin          234000  Seniormanager  Webdevelop
3       205    Steelkunj          316800  Seniormanager  Salesforce

The dataframe data is as follows


The Index is  reset Yes here observe-->

  Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000        manager       Sales
1       102     Reddemer          118000        manager   Analytics
2       200        Scrin          234000        manager  Webdevelop
3       205    Steelkunj          316800        manager  Salesforce
4       101         Sndy          170000  Seniormanager       Sales
5       102     Reddemer          118000  Seniormanager   Analytics
6       200        Scrin          234000  Seniormanager  Webdevelop
7       205    Steelkunj          316800  Seniormanager  Salesforce
--------------- Pandas dataframes CONCAT Horizontally---------------

The dataframe data is as follows


The concat is done on index horizontally-->

  Managerid Manager_Name  Manager_Salary    level    Division Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000  manager       Sales       101         Sndy          170000  Seniormanager       Sales
1       102     Reddemer          118000  manager   Analytics       102     Reddemer          118000  Seniormanager   Analytics
2       200        Scrin          234000  manager  Webdevelop       200        Scrin          234000  Seniormanager  Webdevelop
3       205    Steelkunj          316800  manager  Salesforce       205    Steelkunj          316800  Seniormanager  Salesforce

The dataframe data is as follows


The concat is done on index horizontally  but with joins inside it but  joining on column name/key-->

  Empid    Name  Salary Managerid Managerid Manager_Name  Manager_Salary    level    Division
0     1     Jai   70000       101       101         Sndy          170000  manager       Sales
1     2  Princi   18000       105       102     Reddemer          118000  manager   Analytics
2     3  Gaurav   34000       200       200        Scrin          234000  manager  Webdevelop
3     4    Anuj   16800       200       205    Steelkunj          316800  manager  Salesforce
--------------- Pandas dataframes APPEND---------------
--------------- Pandas dataframes APPEND IS A SPECIAL CASE OF CONCAT---------------
  Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000        manager       Sales
1       102     Reddemer          118000        manager   Analytics
2       200        Scrin          234000        manager  Webdevelop
3       205    Steelkunj          316800        manager  Salesforce
4       101         Sndy          170000  Seniormanager       Sales
5       102     Reddemer          118000  Seniormanager   Analytics
6       200        Scrin          234000  Seniormanager  Webdevelop
7       205    Steelkunj          316800  Seniormanager  Salesforce
--------------- Pandas dataframes MERGE---------------

The dataframe data is as follows

  Empid    Name  Salary Managerid Manager_Name  Manager_Salary    level    Division
0     1     Jai   70000       101         Sndy          170000  manager       Sales
1     3  Gaurav   34000       200        Scrin          234000  manager  Webdevelop
2     4    Anuj   16800       200        Scrin          234000  manager  Webdevelop

The dataframe data is as follows

  Empid    Name  Salary Managerid Manager_Name  Manager_Salary    level    Division
0     1     Jai   70000       101         Sndy          170000  manager       Sales
1     3  Gaurav   34000       200        Scrin          234000  manager  Webdevelop
2     4    Anuj   16800       200        Scrin          234000  manager  Webdevelop

The dataframe data is as follows-->joining with different column names

  Employeeid Name_left  Age    Address Qualification Empid Name_right  Salary Managerid
0          1       Jai   27      Delhi           Msc     1        Jai   70000       101
1          2    Princi   24     Kanpur            MA     2     Princi   18000       105
2          3    Gaurav   22  Allahabad           MCA     3     Gaurav   34000       200
3          4      Anuj   32    Kannauj           Phd     4       Anuj   16800       200

The dataframe data is as follows with different column names and multiple columns

  Employeeid    Name  Age    Address Qualification Empid  Salary Managerid
0          1     Jai   27      Delhi           Msc     1   70000       101
1          2  Princi   24     Kanpur            MA     2   18000       105
2          3  Gaurav   22  Allahabad           MCA     3   34000       200
3          4    Anuj   32    Kannauj           Phd     4   16800       200

FULLOUTER JOIN The dataframe data is as follows with different column names and multiple columns

  Employeeid     Name  Age    Address Qualification Empid   Salary Managerid
0          1      Jai   27      Delhi           Msc     1  70000.0       101
1          2   Princi   24     Kanpur            MA     2  18000.0       105
2          3   Gaurav   22  Allahabad           MCA     3  34000.0       200
3          4     Anuj   32    Kannauj           Phd     4  16800.0       200
4          5  Sandesh   29     Nagpur         Btech   NaN      NaN       NaN

LEFT OUTER JOIN The dataframe data is as follows with different column names and multiple columns

  Employeeid     Name  Age    Address Qualification Empid   Salary Managerid
0          1      Jai   27      Delhi           Msc     1  70000.0       101
1          2   Princi   24     Kanpur            MA     2  18000.0       105
2          3   Gaurav   22  Allahabad           MCA     3  34000.0       200
3          4     Anuj   32    Kannauj           Phd     4  16800.0       200
4          5  Sandesh   29     Nagpur         Btech   NaN      NaN       NaN

RIGHT OUTER JOIN The dataframe data is as follows with different column names and multiple columns

  Employeeid    Name  Age    Address Qualification Empid  Salary Managerid
0          1     Jai   27      Delhi           Msc     1   70000       101
1          2  Princi   24     Kanpur            MA     2   18000       105
2          3  Gaurav   22  Allahabad           MCA     3   34000       200
3          4    Anuj   32    Kannauj           Phd     4   16800       200
--------------- Pandas dataframes JOIN---------------
--------------- Pandas dataframes JOIN IS A SPECIAL CASE OF MERGE---------------
-- Pandas dataframes JOIN does not support left,right outer joins it supports INNER AND FULL OUTER JOIN--

The dataframe data is as follows

  Empid    Name  Salary Managerid Manager_Name  Manager_Salary    level    Division
0     1     Jai   70000       101         Sndy          170000  manager       Sales
1     3  Gaurav   34000       200        Scrin          234000  manager  Webdevelop
2     4    Anuj   16800       200        Scrin          234000  manager  Webdevelop
-----------end-------------------

'''