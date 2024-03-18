import pandas as pd
from data_extraction import DataExtractor
from dateutil.parser import parse
import numpy as np # We will need the `nan` constant from the numpy library to apply to missing values
import re
import math
import pycountry
import pickle

class DataCleaning:
    """ This class contains methods to clean data from each of a set of 3 data sources."""
    def strip_whtespce(df,clmname):
        try:
            df[clmname]=df[clmname].str.strip()
        except:
            pass
    def viewbad_dates(colname, dfname, format="mixed"):
        """This function finds any dates that do not fit the specified format across a specified date column
        within a specified dataframe. The datetime column must be a pandas object data type."""
        count=0
        for i in range(0,len(dfname)-1):
            try:
                dfname.loc[i,colname] = pd.to_datetime(dfname.loc[i,colname], format=format)
            except Exception as er:
                print("Error at position no.: " + str(i) + " Value found: "+ dfname.loc[i,colname])
                count +=1
        print("\n There are "+ str(count) + " errors.")
    def viewbad_numbers(colname, dfname, downcast=None):
        """This function finds any dates that do not fit the specified format across a specified date column
        within a specified dataframe. The datetime column must be a pandas object data type."""
        count=0
        for i in range(0,len(dfname)-1):
            try:
                dfname.loc[i,colname] = pd.to_numeric(dfname.loc[i,colname],errors="raise", downcast=None)
            except Exception as er:
                print("Error at position no.: " + str(i) + " Value found: "+ dfname.loc[i,colname])
                count +=1
        print("\n"+str(count) + " errors found.")
    def viewbad_data(dfname,regex_expression,clmindx="email_address"):
        
        positions=[]
        bad_dataset=[]
        for i in range(0,len(dfname)-1):
            x=re.search(regex_expression,str(dfname.loc[i,clmindx]))
            if bool(x) == False:
                positions.append(i)
                bad_dataset.append(dfname.loc[i,clmindx])
        bad_data=pd.DataFrame(bad_dataset,positions)
        print(bad_data)
        print("\n There are "+str(len(bad_data))+ " errors")
        # print(dfname.loc[positions,"address"])
    def viewcolumns(df, colname=False):
        print(colname)
        if colname== False:
            for clmname in df.columns:
                print(clmname)
                print(df[clmname].head(5))
                x= input("Press enter for next column")
        else:
            print(colname)
            print(df[colname].head(500))
    def view_badcountries(df,display="show"):
        # Obtain list of valid countries
        cntrylist=[]
        for i in (list((pycountry.countries))):
            cntrylist.append(i.name)
        countrydf=pd.DataFrame(cntrylist)

        # Find any invalid countries in df using list of valid countries
        countries_totest= df.country.drop_duplicates()
        badcountries=[]
        for testcountry in countries_totest:
            if not (countrydf.isin([testcountry]).any().any()):
                badcountries.append(testcountry)
        if display=="show":
            print("Bad countries: "+str(badcountries))

        # Remove invalid countries
        for badcountry in badcountries:
            df["country"]=df["country"].replace(badcountry, np.nan)
    def view_badcountrycodes(df,display="show"):
        # Obtain list of valid countries
        cntrylist=[]
        for i in (list((pycountry.countries))):
            cntrylist.append(i.alpha_2)
        validcodes=pd.DataFrame(cntrylist)

        # Find any invalid countries in df using list of valid countries
        codes_totest= df.country_code.drop_duplicates()
        badcodes=[]
        for code in codes_totest:
            if not (validcodes.isin([code]).any().any()):
                badcodes.append(code)
        if display=="show":
            print("Bad country codes: "+str(badcodes))

        # Remove invalid countries
        for code in badcodes:
            df["country_code"]=df["country_code"].replace(code, np.nan)
    def viewduplicates(df,colname):
        duplist=df[colname].duplicated()
        dupindxs=duplist.loc[lambda x : x == True]
        dupindxs=list(dupindxs.index.values)

        duprecords=[]
        for count,i in enumerate(dupindxs):
            record=df.iloc[i]
            duprecords.append(record)
        duprecordsdf = pd.DataFrame(duprecords)
        return duprecordsdf

    def clean_user_data():
        """This method is used for cleaning the data within the legacy_users table."""
        users=DataExtractor.read_rds_table()

        # shape = users.shape
        # table consists up of 12 columns and 15, 320 data records
        
        # users.info()
        # users df has columns of an incorrect data type
        
        # CORRECTING COLUMN DATATYPES OF USERS DATAFRAME & CLEANING DATETYPE COLUMNS
        # users.info()
        for count,clmname in enumerate(users):
            if count==3 or count==10:
                users[clmname] = pd.to_datetime(users[clmname], format="mixed",errors='coerce')
                continue
            elif count==0 or count==6:
                continue
            else:
                users[clmname] = users[clmname].astype('string')
        
        # print(users.dtypes)
        # print(users.iloc[1:3,[4,5,6,7]])
        # users["address"] = users["address"].astype('object')
        # print(users.iloc[1:3,[4,5,6,7]])
        # users.info()

        # users.info()
        # users dataframe now has columns of correct data types.  It appears that some dates
        # were invalid in the users df. These invalid dates can be viewed using viewbad_data methods below.
        
        # CLEANING NAME COLUMNS
        name_rgx_exp="^[a-zA-Z '\u00c4\u00e4\u00d6\u00f6\u00dc\u00fc\u00df\u00e9.-]+$"
        # There are erronous names consisting up of a mixture of numbers and a series of upper case letters.
        # These need to be removed.These invalid names can be viewed using viewbad_data methods below.
        DataCleaning.viewbad_data(users,name_rgx_exp,"first_name")
        # DataCleaning.viewbad_data(users,name_rgx_exp,"last_name")
        users.loc[~users['first_name'].str.match(name_rgx_exp), 'first_name'] = np.nan
        users.loc[~users['last_name'].str.match(name_rgx_exp), 'last_name'] = np.nan

        # CLEANING EMAIL COLUMN       
        email_rgx_exp = '^([a-zA-Z0-9_\-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$' #Our regular expression to match for UK phone no.
        # There are erronous email as can be seen using viewbad_data method. These need to be removed.
        # DataCleaning.viewbad_data(users,email_rgx_exp,"email_address")
        users.loc[~users['email_address'].str.match(email_rgx_exp), 'email_address'] = np.nan 

        #CLEANING ADDRESS COLUMN
        address_rgx_exp=" +"
        users["address"] = users["address"].astype('string')
        # There are erronous emails as can be seen using viewbad_data method. These need to be removed.
        # DataCleaning.viewbad_data(users,address_rgx_exp,"address")
        for count,address in enumerate(users["address"]):
            x=re.search(address_rgx_exp,address)
            if bool(x) == False:
                users.loc[count,"address"]=np.nan
        # Not sure why below commented code doesn't seem to work...
        # users.loc[~users['address'].str.match(address_rgx_exp), 'address'] = "INVALID_ADDRESS"
        users["address"] = users["address"].astype('object')

        #CLEANING COUNTRY COLUMN
        # Below method removes any invalid countries. Also display is set to show, it will output removed
        # countries. Only valid countries are Germany, UK & USA.
        DataCleaning.view_badcountries(users, display="false")
        
        #CLEANING COUNTRY CODE COLUMN
        DataCleaning.view_badcountrycodes(users, display="false")
        
        #CLEANING PHONE NO. COLUMN
        phoneno_exp="^[0-9()+# -]+$"
        # There are erronous phone no. as can be seen using viewbad_data method. These erronous no. typically
        # consist of characters and full stops which shouldn't be found in a phone no. These need to be removed.
        # DataCleaning.viewbad_data(users,phoneno_exp,"phone_number")
        users.loc[~users['phone_number'].str.match(phoneno_exp), 'phone_number'] = np.nan 
        
        #CLEANING UUID COLUMN
        uuid_exp="^([0-9a-z]+)-([0-9a-z]+)-([0-9a-z]+)-([0-9a-z]+)-([0-9a-z]+)$"
        # DataCleaning.viewbad_data(users,uuid_exp,"user_uuid")
        users.loc[~users['user_uuid'].str.match(uuid_exp), 'user_uuid'] = np.nan 

        # For some reason, from original dataframe it appears a record with index no. 773 has been deleted.
        # (Position no. 773 doesn't match index no. 774 as can be seen below.)
        # print(users.iloc[772])
        # print(users.iloc[773])
        
        # uniquevals=users['user_uuid'].nunique()
        # print(uniquevals)
        #returns 15,284

        # view all rows containing null data
        # null_mask = users.isnull().any(axis=1)
        # null_rows = users[null_mask]
        # print(null_rows)

        users.dropna(inplace=True)
        # users.info()
        return users

    def clean_card_data():
        pdflink="https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        card_detailsdf=DataExtractor.retrieve_pdf_data(pdflink)

        # INITIAL INSPECTION
        # print(card_detailsdf.shape)
        # card_detailsdf.info()

        # regex_expression="^[0-9]+$"
        # DataCleaning.viewbad_data(dfname=card_detailsdf,regex_expression=regex_expression,clmindx="card_number")
        # This returns 51 erronous card numbers consisting of letters, question marks and NULL values.
        # card_detailsdf.drop(card_detailsdf.loc[~card_detailsdf['card_number'].str.match(regex_expression)].index, inplace=True) 
        
        # DataCleaning.viewbad_dates(colname="expiry_date", dfname=card_detailsdf, format="%m/%y")
        # There are 25 invalid dates in the expiry_date columns. These errors consist up of Null values
        # and a series of random letters and numbers.
        
        cardprvders=card_detailsdf['card_provider'].value_counts()
        # print(cardprvders)
        # # There appears to be a number of invalid card provider names
        # print(cardprvders.index[cardprvders.gt(11)])
        # # All valid card providers can be seen using above. 
        # print(cardprvders.index[cardprvders.lt(12)])
        # # All invalid card providers can be seen using above. 

        # Replacing all invalid card providers with NaN
        toremove=cardprvders.index[cardprvders.lt(12)]
        for err in toremove:
            # card_detailsdf = card_detailsdf.drop(card_detailsdf[card_detailsdf['card_provider'] == err].index)
            card_detailsdf[card_detailsdf['card_provider'] == err]= np.nan

        # DataCleaning.viewbad_dates(colname="date_payment_confirmed", dfname=card_detailsdf, format="%Y-%m-%d")
        # DataCleaning.viewbad_dates(colname="date_payment_confirmed", dfname=card_detailsdf, format="mixed")
        # There are 33 invalid dates in the date_payment_confirmed columns. These errors consist up of Null 
        # values and a series of random letters and numbers. 8 of these invalid dates are simply in the wrong
        # format. These data formats will need to be converted to %Y-%m-%d. After corrections, 25 remaining
        # invalid dates.
        
        for colname in card_detailsdf.columns:
            if colname=="card_number":
                card_detailsdf[colname] = pd.to_numeric(card_detailsdf[colname],errors='coerce')
                continue
            elif colname=="expiry_date":
                card_detailsdf[colname] = pd.to_datetime(card_detailsdf[colname], format="%m/%y",errors='coerce')
                continue
            elif colname=="date_payment_confirmed":
                card_detailsdf[colname] = pd.to_datetime(card_detailsdf[colname], format="mixed",errors='coerce')
                continue
            else:
                card_detailsdf[colname] = card_detailsdf[colname].astype('string')
        
        # See all rows containing NaN or NaT
        # null_mask = card_detailsdf.isnull().any(axis=1)
        # null_rows = card_detailsdf[null_mask]
        # print(null_rows)
        
        card_detailsdf.dropna(inplace=True)
        card_detailsdf.info()
        return card_detailsdf

    def called_clean_store_data():
        store_data = pd.read_pickle("storedatafile")
        # return store_data
        # with open("TESTINGEXP.txt", "w") as f:
        #     f.write(str(store_data.loc[store_data['store_code']=='WEB-1388012W',:]))

        # INITIAL INSPECTION
        # print(store_data.shape)
        # store_data.info()
        # DataCleaning.viewcolumns(store_data, colname="lat")

        #INSPECTING NUMERICAL DATA
        regex_expression="^[0-9.-]+$"
        # DataCleaning.viewbad_data(dfname=store_data,regex_expression=regex_expression,clmindx="longitude")
        # This returns 11 erronous longitudes consisting of letters and NULL values.

        # DataCleaning.viewbad_data(dfname=store_data,regex_expression=regex_expression,clmindx="lat")
        # There are 450 erronous lat values. These are mainly None Values. Entire column needs dropping.

        # DataCleaning.viewbad_data(dfname=store_data,regex_expression=regex_expression,clmindx="latitude")
        # There are 11 erronous latitude data values.

        # regex_expression="^[0-9]+$"
        # DataCleaning.viewbad_data(dfname=store_data,regex_expression=regex_expression,clmindx="staff_numbers")
        # There are 15 erronous staff number values.

        ##INSPECTING CATEGORICAL DATA
        # localityvals=store_data['locality'].value_counts()
        # pd.set_option('display.max_rows', 500)
        # print(localityvals)
        # with open('readme.txt', 'w') as f:
        #     f.write(str(localityvals))
        
        # The below 7 erronous locality values were manually identified.
        localityerr=["CQMHKI78BX","RY6K0AUE7F","N/A","9IBH8Y4Z0S","1T6B406CI8","6LVWPU1G64","3VHFDNP8ET"]
        
        
        #Replacing all 7 erronous locality values with NaN.
        # store_data=store_data.drop(store_data[store_data['locality'].isin(localityerr)].index)
        store_data.loc[store_data['locality'].isin(localityerr),"locality"]=np.nan
        # print(store_data.loc[store_data['locality'].isnull(),"locality"])
        

        #
        store_typevals=store_data['store_type'].value_counts()
        # print(store_typevals)
        # # There appears to be a number of invalid store_type values
        # print(store_typevals.index[store_typevals.gt(3)])
        # # All valid store_type values can be seen using above. 
        # print(store_typevals.index[store_typevals.lt(4)])
        # # All invalid store_type values can be seen using above. 

        toremove=store_typevals.index[store_typevals.lt(4)]
        for err in toremove:
            # card_detailsdf = card_detailsdf.drop(card_detailsdf[card_detailsdf['card_provider'] == err].index)
            store_data.loc[store_data['store_type'] == err,'store_type']= np.nan
        # print(store_data.loc[store_data['store_type'].isnull(),'store_type'])
        
        country_codevals=store_data['country_code'].value_counts()
        # print(country_codevals)
        # It appears that there are only 3  valid country codes in dataset. There also invalid country codes.
        # print(store_data.loc[store_data['country_code'].isnull(),'country_code'])
        # print(len(store_data.loc[store_data['country_code'].isnull(),'country_code']))
        # However there are 11 NaNs in the country code column.

        toremove=country_codevals.index[country_codevals.lt(4)]
        # print(toremove)
        # quit()
        for err in toremove:
            # card_detailsdf = card_detailsdf.drop(card_detailsdf[card_detailsdf['card_provider'] == err].index)
            store_data.loc[store_data['country_code'] == err,'country_code']= np.nan

        continent_vals=store_data['continent'].value_counts()
        # print(continent_vals)
        # From above it can be seen there are muliple incorrect continent names (e.g. eeAmerica).

        toremove=continent_vals.index[continent_vals.lt(25)]
        for err in toremove:
            # card_detailsdf = card_detailsdf.drop(card_detailsdf[card_detailsdf['card_provider'] == err].index)
            store_data.loc[store_data['continent'] == err,'continent']= np.nan
        # print(store_data.loc[store_data['continent'].isnull(),'continent'])

        regex_expression="^([A-Z]+)-([A-Z0-9]+)$"
        # DataCleaning.viewbad_data(dfname=store_data,regex_expression=regex_expression,clmindx="store_code")
        # There are 10 erronous store codes
        store_data.loc[~store_data['store_code'].str.match(regex_expression),'store_code']=np.nan
        # print(store_data.loc[store_data['store_code'].isnull(),'store_code'])
        

        # print(store_data['opening_date'].head(20))
        # DataCleaning.viewbad_dates(colname="opening_date",dfname=store_data,format="mixed")
        # Columns appear to contain data in mixed formats. There also 10 completely invalid dates.        

        for colname in store_data:
            if colname=="longitude" or colname=="staff_numbers" or colname=="latitude":
                store_data[colname]=pd.to_numeric(store_data[colname],errors="coerce")
            elif colname=="opening_date":
                store_data[colname]=pd.to_datetime(store_data[colname],errors="coerce")
            elif colname=="address":
                continue
            else:
                store_data[colname]=store_data[colname].astype("string")

        # null_mask = store_data.isnull().any(axis=1)
        # null_rows = store_data[null_mask]
        # print(null_rows)
        
        store_data.drop(['lat'], axis=1,inplace=True)
        store_data.dropna(inplace=True)
        # store_data.info()
        return store_data

    def convert_product_weights(dataframe):
        productsdf=dataframe
        # print(productsdf.info()) 
        # pd.set_option("display.max_rows",2000)
        # print(productsdf["weight"])
        # print(productsdf["weight"].head(10))
        
        # Cleaning weights columns has been broken up ino 6 parts
        # PART 1 - Finding all entries containing weights ending in kg or g.
        regexexpression="^[0-9.]+kg$|^[0-9.]+g$"
        goodweights=productsdf.loc[productsdf["weight"].str.match(regexexpression,na=False),"weight"]
        
        #PART 2 - Finding all entries containing weights ending in kg.
        regexexpression="^[0-9.]+kg$"
        goodweightskg=goodweights[goodweights.str.match(regexexpression,na=False)]
        goodweightskg= goodweightskg.str.replace('kg', '')
        goodweightskg=pd.to_numeric(goodweightskg, errors="raise")
        # pd.set_option('display.max_rows',1000)
        # print(goodweightskg)
        
        #PART 3 - Finding all entries containing weights ending in g.
        goodweightsg=goodweights[~goodweights.str.match(regexexpression,na=False)]
        # pd.set_option('display.max_rows',900)
        # print(goodweightsg)
        weightgindices=list(goodweightsg.index.values)
        for count,row in enumerate(goodweightsg):
            numbers = re.findall(r"[0-9.]+",row)
            weightkg= float(numbers[0])/1000
            goodweightsg[weightgindices[count]]=weightkg
        # pd.set_option('display.max_rows',900)
        # print(goodweightsg)
        
        #PART 4 - Finding all entries containing invalid wieghts.
        regexexpression="^[0-9.]+kg$|^[0-9.]+g$"
        badweights=productsdf.loc[~productsdf["weight"].str.match(regexexpression,na=False),"weight"]
        #badweights contains all weight entries not matching the above regexexpression
        # print(badweights)
        # print(len(badweights)) # 46

        #PART 5 - Finding all entries containing values ending in ml.
        regexexpression="^[0-9.]+ml$"
        wrngunits=badweights[badweights.str.match(regexexpression,na=False)]
        unitindices=list(wrngunits.index.values)
        for count,row in enumerate(wrngunits):
            numbers = re.findall(r"[0-9]+",row)
            totalweight= float(numbers[0])/1000
            wrngunits[unitindices[count]]=totalweight
        # print(wrngunits)
            
        #PART 6 - Finding all entries containing values in the format [value] x [value] g
        regexexpression="^[0-9]+ x [0-9]+g"
        wrngfrmts=badweights[badweights.str.match(regexexpression,na=False)]
        frmtsindices=list(wrngfrmts.index.values)
        for count,row in enumerate(wrngfrmts):
            numbers = re.findall(r"[0-9]+",row)
            totalweight= (float(numbers[0])*float(numbers[1]))/1000
            wrngfrmts[frmtsindices[count]]=totalweight
        # print(wrngfrmts)
        
        # Part 7 - Dealing with the remaining bad weight valeus by replacing them with NaN values.
        todrop=unitindices+frmtsindices
        badweights_lftovr=badweights.drop(todrop)
        # print(badweights_lftovr)
        badweights_lftovr[:]=np.nan
        badweights_lftovr[1779]=0.077
        badweights_lftovr[1841]=(16*28.35)/1000 #kilograms=(ounces*28.35)/1000
        # print(badweights_lftovr)

        # Part 8 - Combinning all weight panda series objects & integrating into the weights columns
        # pd.set_option("display.max_rows",5000)
        new_weights=pd.concat([goodweightsg,goodweightskg,wrngfrmts,wrngunits,badweights_lftovr], axis=0)
        # print(new_weights.sort_index(ascending=True))
        # print(new_weights[1800])
        # print(productsdf.loc[1800,"weight"])
        productsdf["weight"]=new_weights
        # print(productsdf["weight"])
        return productsdf["weight"]

    def clean_products_data():
        productsdf=DataExtractor.extract_from_s3()
        productsdf["weight"]=DataCleaning.convert_product_weights(productsdf)

        #INITIAL INSPECTION
        # productsdf.info()
        # print(productsdf.head(7))

        #CLEANING DATA IN product_name
        # with open("productsdata/productnames","w") as f:
        #     f.write(str(list(productsdf["product_name"])))

        #CLEANING DATA IN product_price
        # print(productsdf["product_price"].head(9))
        regex_expression="^£[0-9.]+$"
        # badprices=productsdf.loc[~productsdf["product_price"].str.match(regex_expression, na=False),"product_price"]
        # print(badprices)
        productsdf.loc[~productsdf["product_price"].str.match(regex_expression, na=False),"product_price"]= np.nan

        #CLEANING DATA IN category
        allcategories=productsdf["category"].value_counts()
        # print(allcategories)
        badcategories=list(allcategories[productsdf["category"].value_counts().values<2].index)
        # print(badcategories)

        # print(productsdf.loc[productsdf["category"].isin(badcategories),"category"])
        productsdf.loc[productsdf["category"].isin(badcategories),"category"]=np.nan
        
        #CLEANING DATA IN EAN column
        # print(productsdf["EAN"].head(50))
        regex_expression="^[0-9]+$"
        # badEANS=productsdf.loc[~productsdf["EAN"].str.match(regex_expression, na=False),"EAN"]
        # print(badEANS)
        productsdf.loc[~productsdf["EAN"].str.match(regex_expression, na=False),"EAN"]=np.nan

        #CLEANING DATA IN date_added column
        # print(productsdf.date_added.head(20))
        # DataCleaning.viewbad_dates("date_added",productsdf,format="mixed") # 3 Errors
        productsdf.date_added=pd.to_datetime(productsdf.date_added,format="mixed",errors="coerce")
        
        #CLEANING DATA IN uuid column
        # print(productsdf.uuid.head(20))
        regex_expression="^[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+$"
        # baduuids=productsdf.loc[~productsdf["uuid"].str.match(regex_expression, na=False),"uuid"]
        # print(baduuids)
        productsdf.loc[~productsdf["uuid"].str.match(regex_expression, na=False),"uuid"]=np.nan

        #CLEANING DATA IN removed column (CATEGORICAL)
        # print(productsdf.removed.head(20))
        allremoved=productsdf.removed.value_counts()

        badremovedvals=list(allremoved[productsdf["removed"].value_counts().values<2].index)
        # print(badremovedvals)

        # print(productsdf.loc[productsdf["removed"].isin(badremovedvals),"removed"])
        productsdf.loc[productsdf["removed"].isin(badremovedvals),"removed"]=np.nan
        
        #CLEANING DATA IN product_code column
        # print(productsdf.product_code.head(30))
        regex_expression="^[A-Za-z0-9]+-[A-Za-z0-9]+$"
        # badcodes=productsdf.loc[~productsdf["product_code"].str.match(regex_expression, na=False),"product_code"]
        # print(badcodes)
        productsdf.loc[~productsdf["product_code"].str.match(regex_expression, na=False),"product_code"]=np.nan

        productsdf.dropna(inplace=True)

        for colname in productsdf:
            if colname=="product_name" or colname=="product_price" or  colname=="category" or  colname=="uuid" or colname=="removed" or colname=="product_code":
                productsdf[colname]=productsdf[colname].astype("string")
            if colname=="weight":
                productsdf[colname]=productsdf[colname].astype("float")
            if colname=="EAN":
                productsdf[colname]=pd.to_numeric(productsdf[colname])
        # productsdf.info()
        # print(productsdf)
        return productsdf
        
    def clean_orders_data():
        ordersdf=DataExtractor.read_rds_table(tblename="orders_table")
        
        # INITIAL INSPECTION
        # ordersdf.info()

        # DROPPING COLUMNS WITH POOR DATA
        ordersdf.drop(columns=["first_name","last_name","1"], inplace=True)
        # ordersdf.info()

        #INSPECTING & CLEANING ALL INTEGER TYPE COLUMNS
        #Inspecting & Cleaing level_0 column
        # print(ordersdf.level_0.head(10)) #need to test if all integers
            
        #Inspecting & Cleaing index column
        # print(ordersdf['index'].head(10)) #need to test if all integers
        # DataCleaning.viewbad_numbers(colname='index',dfname=ordersdf, downcast="int64") #No errors

        # #Inspecting & Cleaing card_number column
        # print(ordersdf.card_number.head(10)) #Need to test if all integers
        # DataCleaning.viewbad_numbers(colname='card_number',dfname=ordersdf, downcast="int64") #No errors

        # #Inspecting & Cleaning product_quantity column
        # print(ordersdf.product_quantity.head(10)) #Need to test if all integers
        # DataCleaning.viewbad_numbers(colname='product_quantity',dfname=ordersdf, downcast="int64") #No errors
        

        #INSPECTING & CLEANING ALL OBJECT TYPE COLUMNS
        # #Inspecting & Cleaing date_uuid column
        # print(ordersdf.date_uuid.head(10)) #Need to test to see if in right format
        # regexexpression="^[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+$"
        # print(ordersdf.loc[~ordersdf["date_uuid"].str.match(regexexpression),"date_uuid"]) #empty

        # #Inspecting & Cleaing user_uuid column 
        # print(ordersdf.user_uuid.head(10)) #Need to test to see if in right format 
        # regexexpression="^[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+$"
        # print(ordersdf.loc[~ordersdf["user_uuid"].str.match(regexexpression),"user_uuid"]) #empty

        # #Inspecting & Cleaning store_code column
        # print(ordersdf.store_code.head(10)) #Need to test to see if in right format 
        # regexexpression="^[A-Z]+-[0-9A-Z]+$"
        # print(ordersdf.loc[~ordersdf["store_code"].str.match(regexexpression),"store_code"]) #empty

        # #Inspecting & Cleaing product_code column
        # print(ordersdf.product_code.head(10)) #Need to test to see if in right format 
        # regexexpression="^[A-Z0-9a-z]+-[0-9a-z]+"
        # print(ordersdf.loc[~ordersdf["product_code"].str.match(regexexpression),"product_code"]) #empty

        # ordersdf.info()
        #No bad data found to be removed!!
        return ordersdf

    def clean_dim_date_times():
        # eventsdata=DataExtractor.extractjson_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")
        
        eventsdata = pd.read_pickle("eventsdatafile")
        # eventsdata.info()

        #INSPECTING ALL FIRST 10 ROWS OF EACH COLUMN
        # for colname in eventsdata:
        #     print(colname)
        #     print(eventsdata[colname].head(10))
        #     user=input("Next one?")
        #     print("\n")
        
        #timestamp. All entries here should be in format- hh:mm:ss. Object/ Datetime
        regexexpression="[0-9]{2}:[0-9]{2}:[0-9]{2}"

        #Displaying erronous data
        errors=eventsdata.loc[~eventsdata["timestamp"].str.match(regexexpression),"timestamp"]
        # print(errors)
        # print(str(len(errors))+ " errors found.")
        # percenterror=round(((len(errors)/len(eventsdata))*100),2)
        # print(str(percenterror)+"% of data in this column is erronous")
        
        #Drop erronous data
        eventsdata.drop(errors.index, inplace=True)

        #MONTH. All entries here should be integers varying between 1 and 12. Int
        regexexpression="[0-9]|10|11|12"

        #Displaying erronous data
        errors=eventsdata.loc[~eventsdata["month"].str.match(regexexpression),"month"]
        # print(errors)
        # print(str(len(errors))+ " errors found.")
        # percenterror=round(((len(errors)/len(eventsdata))*100),2)
        # print(str(percenterror)+"% of data in this column is erronous")

        #Drop erronous data
        eventsdata.drop(errors.index, inplace=True)

        #YEAR. All entries here should be integers consisting of 4 digits. Int
        regexexpression="[0-9]{4}"
        
        #Displaying erronous data
        errors=eventsdata.loc[~eventsdata["year"].str.match(regexexpression),"year"]
        # print(errors)
        # print(str(len(errors))+ " errors found.")
        # percenterror=round(((len(errors)/len(eventsdata))*100),2)
        # print(str(percenterror)+"% of data in this column is erronous")

        #Drop erronous data
        eventsdata.drop(errors.index, inplace=True)
        
        #day. All entries here should be integers consisting between 1 and 31. Int
        regexexpression="^(?:[0-9]|[12][0-9]|3[01])$"
        
        #Displaying erronous data
        errors=eventsdata.loc[~eventsdata["day"].str.match(regexexpression),"day"]
        # print(errors)
        # print(str(len(errors))+ " errors found.")
        # percenterror=round(((len(errors)/len(eventsdata))*100),2)
        # print(str(percenterror)+"% of data in this column is erronous")

        #Drop erronous data - 0 errors found
        eventsdata.drop(errors.index, inplace=True)
        
        #time_period. Categorical. All entries should be string with no numbers/punctuations. eg. Morning/Evening.
        # print(eventsdata.time_period.value_counts())
        # 4 categories found-Evening,Midday,Morning,Late_Hours. All appear to be valid strings. No errors found.

        # date_uuid. Should match the form of str-str-str-str-str where str can contain numbers and lowercase letters only.
        regexexpression="[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+-[a-z0-9]+"
        
        #Displaying erronous data
        errors=eventsdata.loc[~eventsdata["date_uuid"].str.match(regexexpression),"date_uuid"]
        # print(errors)
        # print(str(len(errors))+ " errors found.")
        # percenterror=round(((len(errors)/len(eventsdata))*100),2)
        # print(str(percenterror)+"% of data in this column is erronous")

        #Drop erronous data - 0 errors found
        eventsdata.drop(errors.index, inplace=True)

        return eventsdata



# DataCleaning.clean_user_data()
        
# DataCleaning.clean_card_data()

# DataCleaning.called_clean_store_data()

# productsdf=DataExtractor.extract_from_s3()
# DataCleaning.convert_product_weights(dataframe=productsdf)

# DataCleaning.clean_products_data()

# DataCleaning.clean_orders_data()

# DataCleaning.clean_dim_date_times()

# print(DataCleaning.__doc__)
