import pandas as pd 
import numpy as np 
import os
import string # useful for nseindia to remove special characters

import sqlalchemy
import pymysql

status = os.system('mysql.server start')

if status == 0:

    engine = sqlalchemy.create_engine('mysql+pymysql://sec_user:justpass@localhost:3306/securities_master')
    sqlquery = 'select Date, symbolid, AdjClose from phist_nseindia order by symbolid, Date asc'
    df = pd.read_sql(sqlquery, con=engine)
    os.system('mysql.server stop')

# set date to datetime format
df['Date'] = pd.to_datetime(df['Date'],format='%Y-%m-%d')

df['CM5d'] = (df.AdjClose - df.AdjClose.rolling(5).min()) / (df.AdjClose.rolling(5).max() - df.AdjClose.rolling(5).min())
df['CM10d'] = (df.AdjClose - df.AdjClose.rolling(10).min()) / (df.AdjClose.rolling(10).max() - df.AdjClose.rolling(10).min())
df['CM20d'] = (df.AdjClose - df.AdjClose.rolling(20).min()) / (df.AdjClose.rolling(20).max() - df.AdjClose.rolling(20).min())
df['CM50d'] = (df.AdjClose - df.AdjClose.rolling(50).min()) / (df.AdjClose.rolling(50).max() - df.AdjClose.rolling(50).min())
df['CM100d'] = (df.AdjClose - df.AdjClose.rolling(100).min()) / (df.AdjClose.rolling(100).max() - df.AdjClose.rolling(100).min())
df['CM200d'] = (df.AdjClose - df.AdjClose.rolling(200).min()) / (df.AdjClose.rolling(200).max() - df.AdjClose.rolling(200).min())

df['fwdret5d'] = ((df.AdjClose / df.AdjClose.shift(5) -1)*100).shift(-5)
df['fwdret10d'] = ((df.AdjClose / df.AdjClose.shift(10) -1)*100).shift(-10)
df['fwdret20d'] = ((df.AdjClose / df.AdjClose.shift(20) -1)*100).shift(-20)

# now set nan for all values where symbols do not match

df.loc[df['symbolid'] != df['symbolid'].shift(5), 'CM5d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(10), 'CM10d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(20), 'CM20d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(50), 'CM50d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(100), 'CM100d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(200), 'CM200d'] = np.nan

df.loc[df['symbolid'] != df['symbolid'].shift(5), 'fwdret5d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(10), 'fwdret10d'] = np.nan
df.loc[df['symbolid'] != df['symbolid'].shift(20), 'fwdret20d'] = np.nan

df.replace([np.inf, -np.inf], np.nan, inplace=True)

df = df.dropna()

# remove groups where less than 50 stocks
df = df.groupby('Date').filter(lambda x: len(x) > 50)


df['Score'] = df.CM5d * df.CM10d * df.CM20d * df.CM50d * df.CM100d * df.CM200d

df.to_hdf("nselarge.h5", key="df")


paramarr = [ 'CM5d', 'CM10d', 'CM20d', 'CM50d','CM100d', 'CM200d', 'Score']
fwdretstr = 'fwdret5d'

def compute_quantile_returns(df, paramarr, fwdretstr, numquantiles):

    # feature array is scaled from 0 to 1
    # split the dataframe into feature array and return array
    
    # this tells us how granular you want to analyse the data
    bin_edge = 1/ numquantiles

    return_agg = np.zeros((numquantiles, len(paramarr)))
    
    i = 0

    for param in paramarr:
        lowerbound = 0
        upperbound = lowerbound + bin_edge
        j = 0
        while (upperbound <= 1):
            return_agg[j, i] = np.round(np.mean(df[df[paramarr[i]].between(lowerbound, upperbound, inclusive="right")][fwdretstr]),2)
            lowerbound = upperbound
            upperbound = lowerbound + bin_edge
            j = j + 1
        i = i + 1

    return return_agg

def compute_return_quantiles(returns, numquantiles):

    bins 



ret_agg = compute_quantile_returns(df, paramarr, fwdretstr, 25)







