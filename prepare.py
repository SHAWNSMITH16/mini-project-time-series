
import pandas as pd
from datetime import timedelta, datetime

from env import username, host, password
import os

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

def organize(df):
    
    df.sale_date = pd.to_datetime(df.sale_date)
    
    df = df.set_index('sale_date').sort_index()

    return df

def distributions(df):
    
    df.hist()
    
    plt.show()


def add_cols(df):
    
    df['month'] = df.index.strftime('%B')
    
    df['weekday'] = df.index.strftime('%A')
    
    df['sales_total'] = df['sale_amount'] * df['item_price']
    
    return df



def germany_org(df):
    
    df.Date = pd.to_datetime(df.Date)
    
    df = df.set_index('Date').sort_index()

    return df


def germany_cols(df):
    
    df['month'] = df.index.strftime('%B')
    
    df['weekday'] = df.index.strftime('%A')
        
    return df

def fill_germany(df):
    
    df[['Wind', 'Solar', 'Wind+Solar']] = df[['Wind', 'Solar', 'Wind+Solar']].fillna(0)
    
    return df

def germany_distro(df):
    for col in df.columns:
        plt.hist(df[col])
        plt.title(col)
        plt.show()


def get_connection(db, user=username, host=host, password=password):
    
    return f'mysql+pymysql://{user}:{password}@{host}/{db}'


def acquire_store():
    
    filename = 'store.csv'
    
    if os.path.exists(filename):
        
        return pd.read_csv(filename)
    
    else:
        
        query = '''
                SELECT sale_date, sale_amount,
                item_brand, item_name, item_price,
                store_address, store_zipcode
                FROM sales
                LEFT JOIN items USING(item_id)
                LEFT JOIN stores USING(store_id)
                '''
        
        url = get_connection(db='tsa_item_demand')
        
        df = pd.read_sql(query, url)
        
        df.to_csv(filename, index=False)
        
        return df
    
import warnings
warnings.filterwarnings("ignore")