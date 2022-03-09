# DICTIONARY FOR COLUMNS FROM CURATED BUCKET TO BE CHECKED AGAINST RAW VAULT

DICT_STD_COLUMNS=dict(
account=['cover_msg_std','account_number','status_stdd'],
addr=['auth_std','last_changed_by'],
country=['country_id','address']
)

# """   """  is a multi line comment

"""
Dictionary can also be defined as below
DICT_STD_COLUMNS={
account=['cover_msg_std','account_number','status_stdd'],
addr=['auth_std','last_changed_by'],
country=['country_id','address']
}
"""

#DICTIONARY FOR SAT TABLES CORRESPONDING TO ENTITIES

DICT_TABLES = dict(
account='SAT_WH1156_ACCOUNT',
addr='SAT_WH1156_ADDR',
country='SAT_WH1156_COUNTRY'
)

#DICTIONARY FOR PRIMARY KEY COLUMNS CORRESPONDING TO ENTITIES

DICT_PRIMARY_KEY = dict(
account='account_id_std_',
addr='addr_id_std_',
country='country_id_std_'
)

#IMPORT REQUIRED

import psycopg2
import pandas as pd
import s3fs
import pyarrow.parquet as pq
import sys
import pandas.io.sql as psql

#ENTER BELOW THE ENTITIES REQUIRED TO CHECK
entities=['account','addr','country']

#ENTER BELOW THE NUMBER OF RECORDS REQUIRED TO CHECK
number_of_records_to_check = 1

#COMPARE TWO DATAFRAMES FUNCTION
def compare(df1,df2):
	df=pd.concat([df1,df2])
	df=df.reset_index(drop=True)
	df_gpby=df.groupby(list(df.columns))
	idx = [x[0] for x in df_gpby.groups.values() if len(x)==1]
	df.reindex(idx)
	df_diff=pd.concat([df1,df2]).drop_duplicates(keep=False)
	return df_diff
	
#FUNCTION TO GET DATA FROM CURATION BUCKET

def data_from_curated(entity):
		fs=s3fs.S3FileSystem()
		CURATED_BUCKET_PATH='s3://ndhnonprod-sit03-curated-nab/v3/AU/WH1156/DATA/{}/BATCH-1/FULL/year=2021/month=08/day=30'.format(entity)
		dataset=pq.ParquetDataset(CURATED_BUCKET_PATH, filesystem = fs)
		df=dataset.read()
		df=df.to_pandas()
		return df
		
# FUNCTION TO GET DATA FROM STG TABLE

def data_from_stg(entity):
		query="select * from conformance_rawvault_stage.wh1156_{}".format(entity)
		con = psycopg2.connect(dbname='ndhpoc',host='host.com.au',port='8432',user='awsuser',password='Ndhpoc123')
		cur=con.cursor()
		cur.execute(query)
		Data_from_STG = psql.read_sql(query,con)
		return Data_from_STG
		
#FUNCTION TO GET COLUMNS FROM CURATED BUCKET AND RAW VAULT TABLE IN REQUIRED FORMAT

def cols_required(entity):
		cols_required_cur=DICT_STD_COLUMNS(entity)
		cols_required_rv=str(cols_required_cur)
		cols_required_rv=cols_required_rv.replace("[","")
		cols_required_rv=cols_required_rv.replace("]","")
		cols_required_rv=cols_required_rv.replace("'","")
		return cols_required_cur,cols_required_rv
		
#FUNCTION TO GET COLUMNS REQUIRED COLUMNS FOR COMPARISION AGAINST RAW VAULT

def data_from_curated_required_columns(entity):
        cur,rv=cols_required()
        df=data_from_curated()
        df1=df[cur]
        return df1

#FUNCTION TO GET COLUMNS REQUIRED FOR RAW VAULT

def data_from_rv(entity):
         cur,rv=cols_required()
         rv_str=str(rv)
         rv_str_remove_std=rv_str.replace("_std_","")
         query="select {} from conformance_rawvault.{}".format(rv_str_remove_std,DICT_TABLES[entity])
         con = psycopg2.connect(dbname='ndhpoc',host='host.com.au',port='8432',user='awsuser',password='Ndhpoc123')
         cur=con.cursor()
         cur.execute(query)
         Data_from_STG = psql.read_sql(query,con)
         return Data_from_STG
     
# FUNCTION TO CHECK COUNT IN CURATED , STG ANDD RAW VAULT TABLE

def check_count(entity):
        df1=data_from_curated()
        df2=data_from_stg()
        df3=data_from_rv()
        count1=len(df1)
        
        print("\nCount of records in curated bucket : ",count1)
        count2=len(df2)
        stg_print="count of records in STG Table - wh1156_{}".format(entity)
        print("\n")
        print(stg_print,count2)
        count3=len(df3)
        rv_print="Count of records in RV Table - {}:",format(DICT_TABLES[entity])
        print("\n")
        print(rv_print,count3)
        print("\n")
        
#COMPARE CURATION BUCKET VS STG TABLE

def datacheck_curate_stg(entity,REC):
        df11=data_from_curated() 
        cols=df11.columns
        cols_list=cols.tolist()
        df22=data_from_stg(entity)
        
        unique_id=DICT_PRIMARY_KEY[entity]
        cols_list.remove(unique_id)
        
        df1=df11[df11[unique_id]==REC]
        df2=df22[df22[unique_id]==REC]
        
        list = []
        for i in cols_list:
            df_required_cols1=df1[[unique_id,i]].copy()
            
            df_required_cols2=df2[[unique_id,i]].copy()
            
            comp=compare(df_required_cols1,df_required_cols2)
            
            if comp.empty:
                pass
            else:
                list.append(i)
                print("There is a value mismatch for:\t",i)
                print("\nValue of ",i,"in curated bucket:",df1[[i]].iloc[0][i])
                print(type(df1[[i]].iloc[0][i]))
                print("\nValue of ",i,"in STG Table:",df2[[i]].iloc[0][i])
                print(type(df2[[i]].iloc[0][i]))
                print("\n")
                print("-------------------------------------------------------------------------------------")
        
        if not len(list):
            print("Check completed between Curate and STG. There were no record mismatches")
            print("-----------------------------------------------------------------------------")
            print("\n")
        else:
            print("Check completed between Curate and STG. There were record mismatches for these columns :",(",".join(list)))
            print("-----------------------------------------------------------------------------")
            print("\n")
            
# COMPARE CURATION BUCKET VS RAW VAULT TABLE

def datacheck_curate_rv(entity,REC):
    df11=data_from_curated()
    cols=df11.columns
    cols_list=cols.tolist()
    df22=data_from_rv()

    unique_id = DICT_PRIMARY_KEY[entity]
    unique_id_remove_std = unique_id.replace("_std_","")
    cols_list.remove(unique_id)

    df1=df11[df11[unique_id]==REC]
    df2=df22[df22[unique_id_remove_std]==REC]
    
    list=[]
    for i in cols_list:
            df_required_cols1=df1[[unique_id,i]].copy()
            
            i_remove_std=i.replace("_std_","")
            
            df_required_cols2=df2[[unique_id_remove_std,i_remove_std]].copy()
            
            df_required_cols2=df_required_cols2.rename(columns={unique_id_remove_std:unique_id,i_remove_std:i})

            comp=compare(df_required_cols1,df_required_cols2)
            
            if comp.empty:
                pass
            else:
                list.append(i)
                print("There is a value mismatch for:\t",i)
                print("\nValue of ",i,"in curated bucket:",df1[[i]].iloc[0][i])
                print(type(df1[[i]].iloc[0][i]))
                print("\nValue of ",i,"in Raw Vault Table:",df2[[i]].iloc[0][i])
                print(type(df2[[i]].iloc[0][i]))
                print("\n")
                print("-------------------------------------------------------------------------------------")
        
    if not len(list):
            print("Check completed between Curate and Raw Vault Table. There were no record mismatches")
            print("-----------------------------------------------------------------------------")
            print("\n")
    else:
            print("Check completed between Curate and Raw Vault Table. There were record mismatches for these columns :",(",".join(list)))
            print("-----------------------------------------------------------------------------")
            print("\n")    
                
            
def main():
    
    stdoutOrigin=sys.stdout
    sys.sdtout=open("l","w")

    for j in entities:
        try:
            
            print("---------------------------------------------------------------------------")
            print("\n-------------------------------------------------------------------------\n")

            print("\nChecking for Entity :",j)

            print("\n---------------------------------------------------------------------------\n")
            print("\n-------------------------------------------------------------------------\n")
            
            check_count(j)
            
            DF=data_from_curated(j)
            unique_id=DICT_PRIMARY_KEY[j]
            df_id=DF[unique_id]
            
            Length_for_loop = int('{}'.format(number_of_records_to_check))
            
            for i in range(0,Length_for_loop):
                print("---------------------------------------------------------------------------")
                print("VALIDATING RESULTS BETWEEN CURATE BUCKET AND STG FOR RECORD :",df_id[i])
                print("---------------------------------------------------------------------------")
                print("\n")
                datacheck_curate_stg(j, df_id[i])
                
            DF=data_from_curated_required_columns(j)
            unique_id=DICT_PRIMARY_KEY[j]
            df_id=DF[unique_id]
            
            Length_for_loop = int('{}'.format(number_of_records_to_check))
            
            for i in range(0,Length_for_loop):
                print("---------------------------------------------------------------------------")
                print("VALIDATING RESULTS BETWEEN CURATE BUCKET AND Raw Vault table FOR RECORD :",df_id[i])
                print("---------------------------------------------------------------------------")
                print("\n")
                datacheck_curate_rv(j, df_id[i]) 
                
        except Exception as e:
            print("Error occured for :",j)
            print("\nException error ",e)
            continue

    sys.stdout.close()
    sys.stdout=stdoutOrigin
    
main()