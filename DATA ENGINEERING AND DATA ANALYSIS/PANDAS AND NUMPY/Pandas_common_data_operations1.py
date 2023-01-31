from distutils.log import info
from itertools import count
from statistics import median
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
print(f"\nThe dataframe dfemployeeprofile data is as follows \n")
print(dfemployeeprofile)
dfemployeepayroll=pd.DataFrame(employeepayroll)
print(f"\nThe dataframe  dfemployeepayroll data is as follows \n")
print(dfemployeepayroll)
dfManagement=pd.DataFrame(Management)
print(f"\nThe dataframe dfManagement data is as follows \n")
print(dfManagement)
dfSeniorManagement=pd.DataFrame(SeniorManagement)
print(f"\nThe  dfSeniorManagement dataframe data is as follows \n")
print(dfSeniorManagement)

customers_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\customers.csv')
sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')
print("**************************")
"""
print(customers_df.head(10))
print(sales_panda_df.head(10))
print(orders_panda_df.head(10))
"""

print("Joining multiple dataframes -->customers+orders+sales")
customer_order_sales=customers_df.merge(orders_panda_df,on='customer_id',how="inner").merge(sales_panda_df,on="order_id",how="inner")

print(customer_order_sales.columns)

"""
['customer_id', 'customer_name', 'gender', 'age', 'home_address',
       'zip_code', 'city', 'state', 'country', 'order_id', 'payment',
       'order_date', 'delivery_date', 'sales_id', 'product_id',
       'price_per_unit', 'quantity', 'total_price']
"""
print("***********************")
"select sum(quantity),gender,city from data group by gender,city "
print(customer_order_sales.groupby(by=['gender','city'],as_index=False)['quantity'].sum())

"select sum(quantity),SUM(price_per_unit),gender,city from data group by gender,city "
print(customer_order_sales.groupby(by=['gender','city'],as_index=False)['quantity','price_per_unit'].sum())
print("Using group by on multiple columns with multiple conditions with alias")
"""
select sum(price_per_unit),median(price_per_unit),count(order_id),gender,city from data group by gender,city
this is 
"""

agg_df1=customer_order_sales.groupby(by=['gender','city'],as_index=False).agg(
    sum_price=("price_per_unit",sum),
    medium_price=("price_per_unit",sum),
    count_of_order=("order_id",count)

)
print("*************************")
print("Aggregated dataframe group by is displayed")
print(agg_df1)

print("select distinct gender from data")
print("pd.unique(Series)")
print("*************************")
print(f"The distinct genders in data are {pd.unique(customer_order_sales['gender'])}")
print("select count(distinct gender) from data----------- df[columnname].nunique()")
print("*************************")
print("*************nunique is a pandas series method*********")
print(f"The NO OF distinct genders in data are {customer_order_sales['gender'].nunique()}")
print("*************************")
print("select count(*),gender from data group by gender")
print(customer_order_sales['gender'].value_counts())
print("*************************")
print("select count(distinct city),sum(price),mean(cast(age as integer)) ,gender as bonus from data where  zip_code>100 group by  gender")
query_output1=customer_order_sales.groupby(by=['gender'],as_index=False).agg(
count_of_distinct_city=('city',pd.Series.nunique),
sumofprice=('price_per_unit',sum),
mean_of_age=('age',median)
)
[customer_order_sales["zip_code"]>100]
print("*************************")
print(query_output1)
print("*************************")
print("PANDAS ASSIGN METHOD IS VERY IMPORTANT IT ACTS LIKE A WITHCOLUMN functionality")
print("select cast( mean_of_age as integer),sumofprice*1000/50 as bonus_salary,meanofage-4 as serice) from data ")
query_output2=query_output1.astype({"mean_of_age":"int64"}).assign(bonussalary=query_output1["sumofprice"]*1000/50,service=query_output1["mean_of_age"]-4)
print(query_output2.head(10))

print("***********************************")
print("Insurance premium pending years for a specific gender type")
print("select gender,age as currentage ,60-age as premiumtenure from data where ")
query_output3_df=customer_order_sales[["gender","age"]].rename(columns ={"age":"current_age"}).assign(premiumtenure=85-customer_order_sales["age"])

print("***********************************")
print(query_output3_df.head(10))
print("Select distinct city as selected_cities from data where gender=Bigender")
print("***********************************")
query_output4=customer_order_sales[customer_order_sales['gender']=='Bigender'].drop_duplicates(['city'])['city']
print(type(query_output4))
print("***********************************")
print(query_output4)
print("select distinct  gender from data where zipcode>100")

query_output5=customer_order_sales[customer_order_sales["zip_code"]>100]["gender"].drop_duplicates()
print("***********************************")
print(query_output5)
print("***********************************")
print("select distinct  gender from data")
print(customer_order_sales.drop_duplicates("gender")["gender"])
print("***********************************")
print("Working with substrings in pandas dataframe")
print(customer_order_sales['country'].str[:2])
print("***********************************")
print("select distinct ,customer_name ,substring(customer_name,0,4) as stripped_name,substring(,gender,0,3) as stripped_age from data  ")
print("***********************************")
print(customer_order_sales.assign(stripped_name=customer_order_sales['customer_name'].str[0:4],stripped_gender=customer_order_sales['gender'].str[0:3])[['customer_name','stripped_name','stripped_gender']].drop_duplicates().head(10))

print("case when if else application in pandas")
print("OPTION 1 TO USE CASE WHEN USE A FUCNTION OR LAMBDA TO DO IT")

"""
select case when age>30 and gender='Agender' then 'suitable'
         when age<30 then 'true1'
         when age>50 then 'Cancel'
         else Rejected end  as status            from data

"""

def logic_func(d_dataframe):
    if d_dataframe['age']>30 and d_dataframe['gender']=='Agender':
        return 'suitable'
    if d_dataframe['age']<30:
        return 'True1'  
    if d_dataframe['age']>50:
        return 'Cancel'   
    else:
        return 'Rejected'

print("This will return a dataframe with one column which the outputof case when statement")
custom_df1= customer_order_sales.apply(logic_func,axis=1) 
print(custom_df1.head(10))
print("Putting the result of Case when dataframe along with the existing data")
custom_df2=customer_order_sales.assign(status=customer_order_sales.apply(logic_func,axis=1) )
print("***********************************")
print(custom_df2[['age','gender','status']].head(10).drop_duplicates())

print("****************end************")


"""
output
eprecated and slated for removal in Python 3.12. Use setuptools or check PEP 632 for potential alternatives
  from distutils.log import info
Creating  Dataframe from Dictionarys-->


The dataframe dfemployeeprofile data is as follows

  Employeeid     Name  Age    Address Qualification
0          1      Jai   27      Delhi           Msc
1          2   Princi   24     Kanpur            MA
2          3   Gaurav   22  Allahabad           MCA
3          4     Anuj   32    Kannauj           Phd
4          5  Sandesh   29     Nagpur         Btech

The dataframe  dfemployeepayroll data is as follows

  Empid    Name  Salary Managerid
0     1     Jai   70000       101
1     2  Princi   18000       105
2     3  Gaurav   34000       200
3     4    Anuj   16800       200

The dataframe dfManagement data is as follows

  Managerid Manager_Name  Manager_Salary    level    Division
0       101         Sndy          170000  manager       Sales
1       102     Reddemer          118000  manager   Analytics
2       200        Scrin          234000  manager  Webdevelop
3       205    Steelkunj          316800  manager  Salesforce

The  dfSeniorManagement dataframe data is as follows

  Managerid Manager_Name  Manager_Salary          level    Division
0       101         Sndy          170000  Seniormanager       Sales
1       102     Reddemer          118000  Seniormanager   Analytics
2       200        Scrin          234000  Seniormanager  Webdevelop
3       205    Steelkunj          316800  Seniormanager  Salesforce
**************************
Joining multiple dataframes -->customers+orders+sales
Index(['customer_id', 'customer_name', 'gender', 'age', 'home_address',
       'zip_code', 'city', 'state', 'country', 'order_id', 'payment',
       'order_date', 'delivery_date', 'sales_id', 'product_id',
       'price_per_unit', 'quantity', 'total_price'],
      dtype='object')
***********************
         gender               city  quantity
0       Agender         Abbottbury        21
1       Agender        Amelieburgh        23
2       Agender     Bartolettiside         7
3       Agender           Bauchton        10
4       Agender      Callumborough        16
..          ...                ...       ...
608  Polygender  West Benjaminberg        11
609  Polygender      West Eviebury         1
610  Polygender       West Phoenix        19
611  Polygender        West Stella         2
612  Polygender        Zacharybury        31

[613 rows x 3 columns]
d:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022\DATA ENGINEERING AND DATA ANALYSIS\PANDAS AND NUMPY\Pandas_common_data_operations1.py:79: FutureWarning: Indexing with multiple keys (implicitly converted to a tuple of keys) will be deprecated, use a list instead.
  print(customer_order_sales.groupby(by=['gender','city'],as_index=False)['quantity','price_per_unit'].sum())
         gender               city  quantity  price_per_unit
0       Agender         Abbottbury        21            1022
1       Agender        Amelieburgh        23            1366
2       Agender     Bartolettiside         7             402
3       Agender           Bauchton        10             514
4       Agender      Callumborough        16             688
..          ...                ...       ...             ...
608  Polygender  West Benjaminberg        11             516
609  Polygender      West Eviebury         1             112
610  Polygender       West Phoenix        19             928
611  Polygender        West Stella         2              93
612  Polygender        Zacharybury        31            1681

[613 rows x 4 columns]
Using group by on multiple columns with multiple conditions with alias
*************************
Aggregated dataframe group by is displayed
         gender               city  sum_price  medium_price                                   count_of_order
0       Agender         Abbottbury       1022          1022  count(Series([], Name: order_id, dtype: int64))
1       Agender        Amelieburgh       1366          1366  count(Series([], Name: order_id, dtype: int64))
2       Agender     Bartolettiside        402           402  count(Series([], Name: order_id, dtype: int64))
3       Agender           Bauchton        514           514  count(Series([], Name: order_id, dtype: int64))
4       Agender      Callumborough        688           688  count(Series([], Name: order_id, dtype: int64))
..          ...                ...        ...           ...                                              ...
608  Polygender  West Benjaminberg        516           516  count(Series([], Name: order_id, dtype: int64))
609  Polygender      West Eviebury        112           112  count(Series([], Name: order_id, dtype: int64))
610  Polygender       West Phoenix        928           928  count(Series([], Name: order_id, dtype: int64))
611  Polygender        West Stella         93            93  count(Series([], Name: order_id, dtype: int64))
612  Polygender        Zacharybury       1681          1681  count(Series([], Name: order_id, dtype: int64))

[613 rows x 5 columns]
select distinct gender from data
pd.unique(Series)
*************************
The distinct genders in data are ['Female' 'Bigender' 'Agender' 'Male' 'Genderfluid' 'Genderqueer'
 'Non-binary' 'Polygender']
select count(distinct gender) from data----------- df[columnname].nunique()
*************************
*************nunique is a pandas series method*********
The NO OF distinct genders in data are 8
*************************
select count(*),gender from data group by gender
Female         712
Genderfluid    687
Male           672
Genderqueer    647
Polygender     636
Non-binary     598
Agender        526
Bigender       522
Name: gender, dtype: int64
*************************
select count(distinct city),sum(price),mean(cast(age as integer)) ,gender as bonus from data where  zip_code>100 group by  gender
*************************
        gender  count_of_distinct_city  sumofprice  mean_of_age
0      Agender                      76       54517         41.0
1     Bigender                      68       54007         53.0
2       Female                      73       73503         51.0
3  Genderfluid                      87       71151         44.0
4  Genderqueer                      77       66808         49.0
5         Male                      82       69529         48.0
6   Non-binary                      72       61917         55.0
7   Polygender                      78       66076         45.0
*************************
PANDAS ASSIGN METHOD IS VERY IMPORTANT IT ACTS LIKE A WITHCOLUMN functionality
select cast( mean_of_age as integer),sumofprice*1000/50 as bonus_salary,meanofage-4 as serice) from data
        gender  count_of_distinct_city  sumofprice  mean_of_age  bonussalary  service
0      Agender                      76       54517           41    1090340.0     37.0
1     Bigender                      68       54007           53    1080140.0     49.0
2       Female                      73       73503           51    1470060.0     47.0
3  Genderfluid                      87       71151           44    1423020.0     40.0
4  Genderqueer                      77       66808           49    1336160.0     45.0
5         Male                      82       69529           48    1390580.0     44.0
6   Non-binary                      72       61917           55    1238340.0     51.0
7   Polygender                      78       66076           45    1321520.0     41.0
***********************************
Insurance premium pending years for a specific gender type
select gender,age as currentage ,60-age as premiumtenure from data where
***********************************
     gender  current_age  premiumtenure
0    Female           30             55
1    Female           30             55
2    Female           30             55
3    Female           30             55
4    Female           30             55
5    Female           30             55
6    Female           30             55
7    Female           30             55
8  Bigender           76              9
9  Bigender           76              9
Select distinct city as selected_cities from data where gender=Bigender
***********************************
<class 'pandas.core.series.Series'>
***********************************
8            Masonfurt
13           Joelburgh
16         Taylorburgh
167           Eliburgh
260      New Beaumouth
             ...
4630    South Alexview
4695        Haydentown
4793     Lake Owenfurt
4825    New Callumtown
4924       Stewartland
Name: city, Length: 68, dtype: object
select distinct  gender from data where zipcode>100
***********************************
0           Female
8         Bigender
14         Agender
24            Male
27     Genderfluid
63     Genderqueer
72      Non-binary
116     Polygender
Name: gender, dtype: object
***********************************
select distinct  gender from data
0           Female
8         Bigender
14         Agender
24            Male
27     Genderfluid
63     Genderqueer
72      Non-binary
116     Polygender
Name: gender, dtype: object
***********************************
Working with substrings in pandas dataframe
0       Au
1       Au
2       Au
3       Au
4       Au
        ..
4995    Au
4996    Au
4997    Au
4998    Au
4999    Au
Name: country, Length: 5000, dtype: object
***********************************
select distinct ,customer_name ,substring(customer_name,0,4) as stripped_name,substring(,gender,0,3) as stripped_age from data
***********************************
          customer_name stripped_name stripped_gender
0         Leanna Busson          Lean             Fem
8       Winslow Ewbanck          Wins             Big
13    Susanetta Wilshin          Susa             Big
14  Michaeline McIndrew          Mich             Age
16         Fedora Dmych          Fedo             Big
24      Marabel Swinfon          Mara             Mal
27       Avril Rossiter          Avri             Gen
39    Gabbie Aldwinckle          Gabb             Mal
44        Chan Duchesne          Chan             Mal
57     Chadwick Cruddas          Chad             Age
case when if else application in pandas
OPTION 1 TO USE CASE WHEN USE A FUCNTION OR LAMBDA TO DO IT
This will return a dataframe with one column which the outputof case when statement
0    Rejected
1    Rejected
2    Rejected
3    Rejected
4    Rejected
5    Rejected
6    Rejected
7    Rejected
8      Cancel
9      Cancel
dtype: object
Putting the result of Case when dataframe along with the existing data
***********************************
   age    gender    status
0   30    Female  Rejected
8   76  Bigender    Cancel
****************end************

"""