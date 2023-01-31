import pandas as pd

customers_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\customers.csv')
sales_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\sales.csv')
orders_panda_df=pd.read_csv('DATA ENGINEERING AND DATA ANALYSIS\SHOPPING CHART DATA\orders.csv')

print(f"**********************************")
print(f"traversing pandas dataframe ")
print(f"{customers_df.head(5)}")

print(f" dataframe.loc[rows][columns]/ dataframe.loc[rows][columns]")

print(f"{customers_df.iloc[:,:]}")
print(f"*****************************")
print(f"{customers_df.iloc[2:4,3:]}")
print(f"*****************************")
print(f"{customers_df.iloc[[2,4],[0,4]]}")

print(f"Pandas dataframe loc- value based function")

print(f"{customers_df.loc[:,:]}")
print(f"*****************************")
print(f"{customers_df.loc[2:10,'gender']}")
print(f"*****************************")
print(f"{customers_df.loc[[2,5,20],['gender','city']]}")
print(f"*****************************")
print(customers_df.loc[[2,5,20],["customer_name","city"]])

print("****end****")

"""
OUTPUT

PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> & C:/Users/AsusT/AppData/Local/Programs/Python/Python310/python.exe "d:/HERAMB/IMP D DRIVE/COURSES/PYTHON PROJECTS 2022/DATA ENGINEERING AND DATA ANALYSIS/PANDAS AND NUMPY/Pandas_dataframe_iloc_and_loc.py"
**********************************
traversing pandas dataframe 
   customer_id        customer_name       gender  age                    home_address  zip_code               city                         state    country
0            1        Leanna Busson       Female   30  8606 Victoria TerraceSuite 560      5464      Johnstonhaven            Northern Territory  Australia
1            2  Zabrina Harrowsmith  Genderfluid   69      8327 Kirlin SummitApt. 461      8223    New Zacharyfort               South Australia  Australia
2            3      Shina Dullaghan   Polygender   59       269 Gemma SummitSuite 109      5661           Aliburgh  Australian Capital Territory  Australia
3            4        Hewet McVitie     Bigender   67       743 Bailey GroveSuite 141      1729  South Justinhaven                    Queensland  Australia
4            5       Rubia Ashleigh   Polygender   30         48 Hyatt ManorSuite 375      4032     Griffithsshire                    Queensland  Australia
 dataframe.loc[rows][columns]/ dataframe.loc[rows][columns]
     customer_id        customer_name       gender  age                    home_address  zip_code                city                         state    country
0              1        Leanna Busson       Female   30  8606 Victoria TerraceSuite 560      5464       Johnstonhaven            Northern Territory  Australia
1              2  Zabrina Harrowsmith  Genderfluid   69      8327 Kirlin SummitApt. 461      8223     New Zacharyfort               South Australia  Australia
2              3      Shina Dullaghan   Polygender   59       269 Gemma SummitSuite 109      5661            Aliburgh  Australian Capital Territory  Australia
3              4        Hewet McVitie     Bigender   67       743 Bailey GroveSuite 141      1729   South Justinhaven                    Queensland  Australia
4              5       Rubia Ashleigh   Polygender   30         48 Hyatt ManorSuite 375      4032      Griffithsshire                    Queensland  Australia
..           ...                  ...          ...  ...                             ...       ...                 ...                           ...        ...
995          996       Elvira Sarfati      Agender   59    0433 Armstrong HillSuite 974      7613     Lake Danielland                      Tasmania  Australia
996          997       Dickie Grushin   Non-binary   30         04 Howell PassSuite 209      6950         Ellaborough                      Tasmania  Australia
997          998       Rebecka Fabler   Polygender   32       72 Annabelle PassApt. 446        52          Kohlerberg                    Queensland  Australia
998          999       Carita Vynarde   Polygender   30       170 Wilson AvenueApt. 577      7849      East Oscarfurt             Western Australia  Australia
999         1000     Mandel Fairbanks         Male   71      1671 Lauren KnollSuite 945      9012  Lake Audreyborough                      Tasmania  Australia

[1000 rows x 9 columns]
*****************************
   age               home_address  zip_code               city                         state    country
2   59  269 Gemma SummitSuite 109      5661           Aliburgh  Australian Capital Territory  Australia
3   67  743 Bailey GroveSuite 141      1729  South Justinhaven                    Queensland  Australia
*****************************
   customer_id               home_address
2            3  269 Gemma SummitSuite 109
4            5    48 Hyatt ManorSuite 375
Pandas dataframe loc- value based function
     customer_id        customer_name       gender  age                    home_address  zip_code                city                         state    country
0              1        Leanna Busson       Female   30  8606 Victoria TerraceSuite 560      5464       Johnstonhaven            Northern Territory  Australia
1              2  Zabrina Harrowsmith  Genderfluid   69      8327 Kirlin SummitApt. 461      8223     New Zacharyfort               South Australia  Australia
2              3      Shina Dullaghan   Polygender   59       269 Gemma SummitSuite 109      5661            Aliburgh  Australian Capital Territory  Australia
3              4        Hewet McVitie     Bigender   67       743 Bailey GroveSuite 141      1729   South Justinhaven                    Queensland  Australia
4              5       Rubia Ashleigh   Polygender   30         48 Hyatt ManorSuite 375      4032      Griffithsshire                    Queensland  Australia
..           ...                  ...          ...  ...                             ...       ...                 ...                           ...        ...
995          996       Elvira Sarfati      Agender   59    0433 Armstrong HillSuite 974      7613     Lake Danielland                      Tasmania  Australia
996          997       Dickie Grushin   Non-binary   30         04 Howell PassSuite 209      6950         Ellaborough                      Tasmania  Australia
997          998       Rebecka Fabler   Polygender   32       72 Annabelle PassApt. 446        52          Kohlerberg                    Queensland  Australia
998          999       Carita Vynarde   Polygender   30       170 Wilson AvenueApt. 577      7849      East Oscarfurt             Western Australia  Australia
999         1000     Mandel Fairbanks         Male   71      1671 Lauren KnollSuite 945      9012  Lake Audreyborough                      Tasmania  Australia

[1000 rows x 9 columns]
*****************************
2      Polygender
3        Bigender
4      Polygender
5     Genderfluid
6        Bigender
7         Agender
8            Male
9        Bigender
10        Agender
Name: gender, dtype: object
*****************************
         gender          city
2    Polygender      Aliburgh
5   Genderfluid    Blakehaven
20   Non-binary  South Callum
*****************************
      customer_name          city
2   Shina Dullaghan      Aliburgh
5    Cordey Tolcher    Blakehaven
20    Seana Hearons  South Callum
****end****
PS D:\HERAMB\IMP D DRIVE\COURSES\PYTHON PROJECTS 2022> 


"""