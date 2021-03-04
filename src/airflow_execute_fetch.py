import os
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

from datetime import timedelta
import datetime as dt

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


## Python Code
def get_new_data():
    import yfinance as yf
    import pandas as pd
    import numpy as np

    date_string = dt.datetime.now().strftime("%Y-%m-%d")
    
    # global finance data
    sp500 = yf.Ticker('^GSPC')
    vix = yf.Ticker('^VIX')
    vix3m = yf.Ticker('^VIX3M')

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

    return result.to_json(orient='records')

def save_data_to_database(task_instance):
    import pandas as pd
    import numpy as np
    
    data = task_instance.xcom_pull(task_ids='fetch-new-data')
    data = pd.read_json(data, orient='records')	
    filename = '/app/airflow/findat/database.csv'
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

#def fetch_data():
#    data = get_new_data()
#    save_data_to_database(data, '/app/airflow/findat/database.csv')

## DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'execute-findata-fetch',
    default_args=default_args,
    schedule_interval='30 7 * * *',
    catchup=False,
)

fetch_new_data = PythonOperator(
    task_id='fetch-new-data',
    python_callable=get_new_data,
    dag=dag,
)

store_new_data = PythonOperator(
   task_id='store-new-data',
   python_callable=save_data_to_database,
   dag=dag
)

fetch_new_data >> store_new_data
