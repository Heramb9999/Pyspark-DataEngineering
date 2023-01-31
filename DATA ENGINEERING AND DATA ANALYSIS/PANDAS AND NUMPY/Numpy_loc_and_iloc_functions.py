import pandas as pd


sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')
#product_id can be treated as primary key of the dataset 
print(sales_panda_df.head(10))

series_dict={"name":"lucifer","age":"30","profession":"gamer"}

print(f"\n Series made from dataframe \n ")
pdseries1=sales_panda_df["product_id"]
print(f"********************** {type(pdseries1)}  *****************")
print(f"\n Series made from dictionary \n ")
pdseries2=pd.Series(series_dict)
print(f"\n********************** {type(pdseries2)}  *****************\n")
print(pdseries2)
print(f"\n Series traversing -loc and iloc demo \n ")
print(f"iloc function ")
print(f"\n printing the first 10 rows of pandas series\n ")
print(pdseries1.iloc[0:10])
print(f"\n printing the 14th row of pandas series\n ")
print(pdseries1.iloc[8])
print(f"loc function ")

print(f"\n printing the 3rd row of pandas series-Series created from dictionary\n ")
print(f"\n******************************")
print(pdseries2.loc["profession"])
print(f"\n******************************")
print(pdseries2.loc["name"])

print(f"\n************end****************")

"""
OUTPUT
ERING AND DATA ANALYSIS/PANDAS AND NUMPY/Numpy_loc_and_iloc_functions.py"
   sales_id  order_id  product_id  price_per_unit  quantity  total_price
0         0         1         218             106         2          212
1         1         1         481             118         1          118
2         2         1           2              96         3          288
3         3         1        1002             106         2          212
4         4         1         691             113         3          339
5         5         1         981             106         3          318
6         6         2         915              96         1           96
7         7         2         686             113         1          113
8         8         2        1091             115         3          345
9         9         2        1196             105         1          105

 Series made from dataframe

********************** <class 'pandas.core.series.Series'>  *****************

 Series made from dictionary


********************** <class 'pandas.core.series.Series'>  *****************

name          lucifer
age                30
profession      gamer
dtype: object

 Series traversing -loc and iloc demo

iloc function

 printing the first 10 rows of pandas series

0     218
1     481
2       2
3    1002
4     691
5     981
6     915
7     686
8    1091
9    1196
Name: product_id, dtype: int64

 printing the 14th row of pandas series

1091
loc function

 printing the 3rd row of pandas series-Series created from dictionary


******************************
gamer

******************************
lucifer

************end****************
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> 


"""

