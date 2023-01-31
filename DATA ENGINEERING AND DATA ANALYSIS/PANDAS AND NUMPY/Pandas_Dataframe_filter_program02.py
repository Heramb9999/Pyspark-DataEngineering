import pandas as pd

print(f"Pandas Dataframe Filtering")
customers_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\customers.csv')
sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')
print("\nextracting few columns from the huge dataset\n")
customer_df_short=customers_df[["customer_id","customer_name","gender","age"]]
print(customer_df_short.head(5))
#Boolean mask-method it returns true or false depending on condition
print("****************")
print(customer_df_short["customer_id"]>10)
print("****************")
print(f"\nFilter approach1\n")
print(customer_df_short[customer_df_short["customer_id"]<5])
print(f"\nFilter approach2 by using a variable\n")
filter1=customer_df_short["customer_id"]<5
print("****************")
print(customer_df_short[filter1])
print("*** complex conditions ***** ")
filter2=customer_df_short["gender"].isin(["Female","Polygender"])
filter3=(customer_df_short["customer_id"]<3)|(customer_df_short["gender"]=="Female")
print("***********simpled way*****")
print(customer_df_short[(filter2)|(filter3)].head(5))
print("***********complex way-it becomes confusing*****")
print(customer_df_short[(customer_df_short["customer_id"]<3)|(customer_df_short["gender"]=="Female")].head(5))
print("\n**********end********\n")
"""
OUTPUT
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> & C:/Users/AsusT/AppData/Local/Programs/Python/Python310/python.exe "d:/HERAMB/IMP D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PANDAS AND NUMPY/Pandas_Dataframe_filter_program02.py"
Pandas Dataframe Filtering

extracting few columns from the huge dataset

   customer_id        customer_name       gender  age
0            1        Leanna Busson       Female   30
1            2  Zabrina Harrowsmith  Genderfluid   69
2            3      Shina Dullaghan   Polygender   59
3            4        Hewet McVitie     Bigender   67
4            5       Rubia Ashleigh   Polygender   30
****************
0      False
1      False
2      False
3      False
4      False
       ...
995     True
996     True
997     True
998     True
999     True
Name: customer_id, Length: 1000, dtype: bool
****************

Filter approach1

   customer_id        customer_name       gender  age
0            1        Leanna Busson       Female   30
1            2  Zabrina Harrowsmith  Genderfluid   69
2            3      Shina Dullaghan   Polygender   59
3            4        Hewet McVitie     Bigender   67

Filter approach2 by using a variable

****************
   customer_id        customer_name       gender  age
0            1        Leanna Busson       Female   30
1            2  Zabrina Harrowsmith  Genderfluid   69
2            3      Shina Dullaghan   Polygender   59
3            4        Hewet McVitie     Bigender   67
*** complex conditions *****
***********simpled way*****
    customer_id        customer_name       gender  age
0             1        Leanna Busson       Female   30
1             2  Zabrina Harrowsmith  Genderfluid   69
2             3      Shina Dullaghan   Polygender   59
4             5       Rubia Ashleigh   Polygender   30
22           23        Alair Grimwad       Female   32
***********complex way-it becomes confusing*****
    customer_id        customer_name       gender  age
0             1        Leanna Busson       Female   30
1             2  Zabrina Harrowsmith  Genderfluid   69
22           23        Alair Grimwad       Female   32
29           30      Bernice Grindle       Female   38
48           49      Shanna Pietrzyk       Female   56

**********end********

"""