import pymongo
import pandas as pd
import pymongo
from dagster import get_dagster_logger, op, job
from pymongo import MongoClient
from sqlalchemy import create_engine


@op
def get_raw_data():
    print('Getting raw data !!!')
    conn = pymongo.MongoClient('localhost', 27017)
    collection = conn['GDP_STG']['SRC_STG_GDP_CUR']
    cursor = collection.find()
    list_cur = list(cursor)
    print('Getting raw data --> Done !!!')
    return list_cur


@op
def flatten_DF(df):
    print('Flattening raw data !!!')
    p = (pd.json_normalize(df, max_level=2))
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


# Create country dimension

@op
def create_country_dim(df):
    country_dim = df[['country_code', 'country_name']]
    return country_dim


# create dates dimension

@op
def create_dates_dim(df):
    dates_dim = df[['year']]
    return dates_dim


@op
# create current_gdp fact
def create_cur_gdp_f(df):
    cur_gdp_f = df[['country_code', 'year', 'indicator_id', 'indicator_name', 'value']]
    return cur_gdp_f


@op
# insert into postgres datawarehouse
def ins_stg_dw(country_dim,dates_dim,gdp_f):
    engine = create_engine('postgresql://postgres:admin@localhost:5432/gdp_data')
    gdp_f.to_sql('gdp_cur_f', engine)
    dates_dim.to_sql('dates_dim', engine)
    country_dim.to_sql('country_dim', engine)
    print('Data Inserted')


# @op
# def main():
#     print('Staring Execution now !!!')
#     raw_js_list = get_raw_data('GDP_STG' ,'SRC_STG_GDP_CUR')
#     flat_df = flatten_DF(raw_js_list)
#     src_data = rename_columns(flat_df)
#     country_dim = create_country_dim(src_data)
#     dates_dim = create_dates_dim(src_data)
#     gdp_f = create_cur_gdp_f(src_data)
#     print(gdp_f)
#     print(src_data)
#     x = type(get_raw_data())
#     y = flatten_DF(x)
#     z = y.head()
#     print(x)
#     z = rename_columns(y)
#     z.head()

@job
def gdp_etl_pipeline():
    # db_name = 'GDP_STG'
    # col_name = 'SRC_STG_GDP_CUR'
    raw_data = get_raw_data()
    flat_df = flatten_DF(raw_data)
    src_df = rename_columns(flat_df)
    country_dim = create_country_dim(src_df)
    dates_dim = create_dates_dim(src_df)
    gdp_f = create_cur_gdp_f(src_df)
    ins_stg_dw(country_dim,dates_dim,gdp_f)



