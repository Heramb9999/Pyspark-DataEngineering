import pandas as pd

customers_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\customers.csv')
sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')

print("***Printing the customers dataframe****")
print(customers_df[['customer_id','gender','age']].head(10))
print("****checking the exiting datatypes*****")
print(customers_df[['customer_id','gender','age']].head(10).dtypes)

print("Casting the datatype of dataframe changing age from int64 to string" )
dataframe2=customers_df[['customer_id','gender','age']].head(10)
print("changing the Datatype of one column at a time in pandas dataframe ")
dataframe3=dataframe2['age'].astype('str')
print(dataframe3)
print("How to change the datatype of multiple columns of a pandas dataframe")
print("Existing Dataframe")
print(dataframe2)
print(" Dataframe after applying datatype cast operation on  multiple columns-here we need to pass a dictionary")
new_customers_df1=dataframe2.astype({"customer_id":"str","gender":"category","age":"str"})
print("*********************note Object datatype means string datatype in pandas***")
print(new_customers_df1.dtypes)

print("*********PANDAS AUTO CAST METHOD-convert dtypes() *************")
dataframe2=dataframe2.convert_dtypes()
print(dataframe2.dtypes)

"""
output
ERING AND DATA ANALYSIS/PANDAS AND NUMPY/Pandas_Dataframe_Castdatatype.py"
***Printing the customers dataframe****
   customer_id       gender  age
0            1       Female   30
1            2  Genderfluid   69
2            3   Polygender   59
3            4     Bigender   67
4            5   Polygender   30
5            6  Genderfluid   40
6            7     Bigender   76
7            8      Agender   75
8            9         Male   51
9           10     Bigender   70
****checking the exiting datatypes*****
customer_id     int64
gender         object
age             int64
dtype: object
Casting the datatype of dataframe changing age from int64 to string
changing the Datatype of one column at a time in pandas dataframe
0    30
1    69
2    59
3    67
4    30
5    40
6    76
7    75
8    51
9    70
Name: age, dtype: object
How to change the datatype of multiple columns of a pandas dataframe
Existing Dataframe
   customer_id       gender  age
0            1       Female   30
1            2  Genderfluid   69
2            3   Polygender   59
3            4     Bigender   67
4            5   Polygender   30
5            6  Genderfluid   40
6            7     Bigender   76
7            8      Agender   75
8            9         Male   51
9           10     Bigender   70
 Dataframe after applying datatype cast operation on  multiple columns-here we need to pass a dictionary
*********************note Object datatype means string datatype in pandas***
customer_id      object
gender         category
age              object
dtype: object
*********PANDAS AUTO CAST METHOD-convert dtypes() *************
customer_id     Int64
gender         string
age             Int64
dtype: object
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> 


"""