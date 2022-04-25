
import pymongo
import pprint
import pandas as pd
import pymongo
import psycopg2
from dagster import get_dagster_logger, op, job
from pymongo import MongoClient
from sqlalchemy import create_engine
@op
def get_raw_data_C_TOT():
    print('Getting raw data !!!')
    conn = pymongo.MongoClient('localhost', 27017)
    collection = conn['ENV_STG']['STG_C_TOT_EMISSION']
    cursor = collection.find()
    list_cur = list(cursor)
    return list_cur

@op
def get_raw_data_C_PC():
    print('Getting raw data !!!')
    conn = pymongo.MongoClient('localhost', 27017)
    collection = conn['ENV_STG']['STG_C_PC_EMISSION']
    cursor = collection.find()
    list_cur = list(cursor)
    return list_cur

@op
def get_raw_data_METH():
    print('Getting raw data !!!')
    conn = pymongo.MongoClient('localhost', 27017)
    collection = conn['ENV_STG']['STG_METH_EMISSION']
    cursor = collection.find()
    list_cur = list(cursor)
    return list_cur

@op
def get_raw_data_GHG():
    print('Getting raw data !!!')
    conn = pymongo.MongoClient('localhost', 27017)
    collection = conn['ENV_STG']['STG_GHG_EMISSION']
    cursor = collection.find()
    list_cur = list(cursor)
    return list_cur

@op
def get_raw_data_FOREST_COVER():
    print('Getting raw data !!!')
    conn = pymongo.MongoClient('localhost', 27017)
    collection = conn['ENV_STG']['STG_FOREST_COVER']
    cursor = collection.find()
    list_cur = list(cursor)
    return list_cur

@op
def get_temperature_data():
    conn = psycopg2.connect('postgresql://postgres:admin@localhost:5432/Stg_Climate')
    temp_df = pd.read_sql_query('SELECT * FROM public.stg_temperature',con=conn)
    country_df = pd.read_csv('Metadata_Country.csv')
    h= temp_df.iloc[:,1:]
    l = pd.melt(h, id_vars=['Year'], var_name=['country'])
    l['indicator_id'] = 'temperature'
    l['indicator_name'] = 'Mean Global Temperature'
    # l.drop(['index'], axis = 1 , inplace = True)
    l = pd.merge(l, country_df,left_on=['country'] , right_on = ['TableName'] ,how='inner')
    temperature_df =l[['Country_Code','Year','indicator_id','indicator_name','value']]
    temperature_df.columns =['country_code', 'year', 'indicator_id', 'indicator_name', 'value']
    return temperature_df

@op
def flatten_DF(df):
    print('Flattening raw data !!!')
    p = (pd.json_normalize(df))
    flat_df = pd.DataFrame(p)
    print('Flattening raw data --> Done !!!')
    return flat_df

@op
def rename_columns(df):
    print('Renaming child columns!!!')
    src = df[['countryiso3code', 'date', 'value', 'indicator.id', 'indicator.value', 'country.value']]
    src.rename(columns={'countryiso3code': 'country_code', 'date': 'year', 'indicator.id': 'indicator_id',
    'indicator.value': 'indicator_name', 'country.value': 'country_name'}, inplace=True)
    print('Renaming child columns --> Done !!!')
    return src

@op
def create_fact(df):
    fact_leb_df = df[['country_code', 'year', 'indicator_id', 'indicator_name', 'value']]
    return fact_leb_df

@op
# insert into postgres datawarehouse
def ins_stg_dw(fact_df_C_TOT,fact_df_C_PC,fact_df_METH,fact_df_GHG,fact_df_FOREST_COV,fact_temp):
    df_ins = fact_df_C_TOT.append([fact_df_C_PC,fact_df_METH,fact_df_GHG,fact_df_FOREST_COV,fact_temp])
    engine1 = create_engine('postgresql://postgres:admin@localhost:5432/ENV_Data')
    df_ins.to_sql('ENV_IND_F', engine1) print('Data Inserted')

@job
def env_etl_pipeline():
    raw_data_C_TOT= get_raw_data_C_TOT()
    raw_data_C_PC = get_raw_data_C_PC()
    raw_data_METH = get_raw_data_METH()
    raw_data_GHG = get_raw_data_GHG()
    raw_data_FC = get_raw_data_FOREST_COVER()
    fact_temp = get_temperature_data() ins_stg_dw(
    fact_df_C_TOT = create_fact(rename_columns(flatten_DF(raw_data_C_TOT))
    ,fact_df_C_PC = create_fact(rename_columns(flatten_DF(raw_data_C_PC))
    ,fact_df_METH = create_fact(rename_columns(flatten_DF(raw_data_METH))
    ,fact_df_GHG = create_fact(rename_columns(flatten_DF(raw_data_METH))
    ,fact_df_FOREST_COV = create_fact(rename_columns(flatten_DF(raw_data_METH))
    ,fact_temp
    )

