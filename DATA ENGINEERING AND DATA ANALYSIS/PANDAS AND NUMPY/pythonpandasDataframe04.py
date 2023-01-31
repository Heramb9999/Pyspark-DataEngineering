import collections
import numpy as np
import pandas as pd

filedatadf=pd.read_csv('product.csv')
print(f"The dataframe has NA values there are differnt ways to handle NA values \n")
print(f"The dataframe data is as follows \n")

print(filedatadf)
filedatadf.insert(7,"newcol","NaN")
#filedatadf=filedatadf[]
print(f"The dataframe data is as follows \n")

print(filedatadf)
print(f"finding whether  dataframe data has NA null values using isnull function is as follows \n")
print(filedatadf.isnull())
print(f"finding the list of columns in which the  dataframe data has NA null values using isnull function is as follows \n")
print(list(filedatadf.isnull().columns))
print(f"finding the list of rows in which the  dataframe data has NA null values using isnull function is as follows \n")
print(filedatadf["PRODUCT_ID"].isnull().count())

print(f"finding the list of rows in ALL COLUMNS  which the  dataframe data has NA null values using isnull function is as follows \n")
Filter1forallcolumns=(filedatadf["PRODUCT_ID"].isnull())|(filedatadf["MANUFACTURER"].isnull())|(filedatadf["DEPARTMENT"].isnull())|(filedatadf["BRAND"].isnull())|(filedatadf["BRAND"].isnull())|(filedatadf["COMMODITY_DESC"].isnull())|(filedatadf["SUB_COMMODITY_DESC"].isnull())
print(filedatadf[Filter1forallcolumns].count())

print(f"fILLING THE NaN values from  dataframe data with some values using FILLNA METHOD is as follows \n")
filedatadf=filedatadf.loc[:,"newcol"].fillna('FILLINGTHEVALUE',inplace=False)
print(f"The dataframe data is as follows \n")
print(filedatadf)
print("making new data frame with dropped NA values with dropna function")
new_data = filedatadf.dropna(axis = 0, how ='any')
print(f"The dataframe data is as follows \n")
print(filedatadf)

