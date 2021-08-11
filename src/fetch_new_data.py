#!/usr/bin/env python3
import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

import yfinance as yf
import pandas as pd
import datetime as dt
import numpy as np

def get_new_data():
    date_string = dt.datetime.now().strftime("%Y-%m-%d")
    
    # global finance data
    sp500 = yf.Ticker('^GSPC')
    vix = yf.Ticker('^VIX')
    vix3m = yf.Ticker('^VIX3M')
    
    #all_world_etf = yf.Ticker('VWRL.AS')
    #print(all_world_etf.info['regularMarketPreviousClose'])

    # Put/Call Ratios
    cboe_data = pd.read_html('https://markets.cboe.com/us/options/market_statistics/daily/')[0]
    cboe_data.columns = ['NAME', 'RATIO']

    # result DataFrame
    result = pd.DataFrame( {
                        'Date': pd.to_datetime(date_string),
                        'SP500':float( sp500.info['previousClose'] ), 
                        'VIX': float( vix.info['previousClose'] ),
                        'VIX3M': float( vix3m.info['previousClose'] ),
                        'TOTAL_PCR': float( cboe_data[ cboe_data.NAME == 'TOTAL PUT/CALL RATIO']['RATIO'].values[0] ), 
                        'INDEX_PCR': float( cboe_data[ cboe_data.NAME == 'INDEX PUT/CALL RATIO']['RATIO'].values[0] ),
                        'EQUITY_PCR': float( cboe_data[ cboe_data.NAME == 'EQUITY PUT/CALL RATIO']['RATIO'].values[0] ),
                        'VIX_PCR':float( cboe_data[ cboe_data.NAME == 'CBOE VOLATILITY INDEX (VIX) PUT/CALL RATIO']['RATIO'].values[0] )
                    }, index=[0])
    print(result)

    return result

def save_data_to_database(data, filename):
    # create empty file with correct headers if necessary
    if os.path.exists(filename) is not True:
        pd.DataFrame( {
                        'Date': np.datetime64(),
                        'SP500':float(), 
                        'VIX': float( ),
                        'VIX3M': float( ),
                        'TOTAL_PCR': float(), 
                        'INDEX_PCR': float(),
                        'EQUITY_PCR': float(),
                        'VIX_PCR':float()
                    }, index=[]).to_csv(filename, sep=';', decimal='.', encoding='utf-8', index=False)
    
    # add new entry
    df = pd.read_csv(filename, sep=';', decimal='.', encoding='utf-8', parse_dates=['Date'])
    df = df.append(data).reset_index(drop=True)
    
    # check/remove duplicates in date column
    df = df.drop_duplicates(subset=['Date'], keep='last')

    # save to file
    df.to_csv(filename, sep=';', decimal='.', encoding='utf-8', index=False)

def main():
    data = get_new_data()
    save_data_to_database(data, 'data/database.csv')

if __name__ == "__main__":
    # we assume this code is in /src while data is in /data. Since we do not want to assume a cwd we switch to src and than move one up
    path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path)
    os.chdir(os.path.pardir)

    main()
