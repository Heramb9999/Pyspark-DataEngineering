import numpy as np
import pandas as pd

filedata=pd.read_csv('product.csv')
# method1 boolean mask

filedata["MANUFACTURER"]==2

# Putting advance complex filters
(filedata["MANUFACTURER"]==2) & (filedata["BRAND"]=="Private") | (filedata["PRODUCT_ID"]>3000)
# In boolean method we put this filter inside a dataframe and it returns all values satistying the condition 
filedata[(filedata["MANUFACTURER"]==2) & (filedata["BRAND"]=="Private") | (filedata["PRODUCT_ID"]>3000)]
# example2
filedata[(filedata["PRODUCT_ID"]>30000) & (filedata["PRODUCT_ID"]<30500)]

# Method 2 to filter the data in df is using Loc here in output we can select desired/ columns that we want to display
# EXAMPLE1
filter=(filedata["PRODUCT_ID"]>30000) & (filedata["PRODUCT_ID"]<30500)
filedata.loc[filter,["PRODUCT_ID","BRAND","SUB_COMMODITY_DESC"]]
# #EXAMPLE2
filter=filedata["DEPARTMENT"].isin(['GROCERY','PASTRY','MEAT-PCKGD'])
filedata.loc[filter,["PRODUCT_ID","DEPARTMENT","SUB_COMMODITY_DESC"]]
# WAY 03- USING QUERY METHOD least used
#Example1
df1=filedata.query("(PRODUCT_ID>30000) & (PRODUCT_ID<30500)",inplace=False)
print(df1)

