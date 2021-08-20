import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt
import seaborn as sns

def load_data_from_database(filename):
    return pd.read_csv(filename, sep=';', decimal='.', encoding='utf-8', parse_dates=['Date'])

def plot_indicator(data, indicator):
    plt.figure(figsize=(20,10))
    plt.plot(data['Date'], data[indicator], marker='', markersize=2, linestyle=':', linewidth=1, color='r', label=indicator)
    plt.plot(data['Date'], data[indicator].rolling(7).mean(), label=indicator + '_7D')
    plt.plot(data['Date'], data[indicator].rolling(21).mean(), label=indicator + '_14D')
    plt.plot(data['Date'], data[indicator].ewm(alpha=0.001).mean(), label=indicator + '_EMA')

    # try:
    #     data = data.set_index('Date')
    #     ts = data[indicator].resample('H')
    #     ts = ts.interpolate(method='polynomial', order=3)
    #     ts.plot()
    # except:
    #     pass

    plt.title(indicator)
    plt.legend()

    try:
        os.mkdir('plots/' + dt.datetime.now().strftime("%Y-%m-%d")+'/')
    except:
        pass

    plt.savefig('plots/' + dt.datetime.now().strftime("%Y-%m-%d")+'/' + indicator + '.png', dpi=300)
    plt.close()

def plot_data(data):
    for indicator in data.columns:
        if indicator == 'Date':
            continue
        plot_indicator(data, indicator)

def compute_indicators(data):
    data['VIX3M_VIX'] = data['VIX3M'] / data['VIX']
    data['VIX3M-VIX'] = data['VIX3M'] - data['VIX']
    return data

if __name__ == "__main__":
    # we assume this code is in /src/plotting while data is in /data. Since we do not want to assume a cwd we switch to src and than move one up
    path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path)
    os.chdir(os.path.pardir)
    os.chdir(os.path.pardir)

    data = load_data_from_database('data/database.csv')
    data = compute_indicators(data)
    plot_data(data)