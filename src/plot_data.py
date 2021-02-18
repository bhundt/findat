import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime as dt

def load_data_from_database(filename):
    return pd.read_csv(filename, sep=';', decimal='.', encoding='utf-8', parse_dates=['Date'])

def plot_data(data):
    fig, host = plt.subplots(figsize=(12,7)) # (width, height) in inches
    # (see https://matplotlib.org/3.3.3/api/_as_gen/matplotlib.pyplot.subplots.html)
        
    par1 = host.twinx()
    par2 = host.twinx()
        
    #host.set_xlim()
    pcr_min, pcr_max = data[['TOTAL_PCR', 'INDEX_PCR', 'EQUITY_PCR', 'VIX_PCR']].min().min(), data[['TOTAL_PCR', 'INDEX_PCR', 'EQUITY_PCR', 'VIX_PCR']].max().max()
    vix_min, vix_max = data[['VIX', 'VIX3M']].min().min(), data[['VIX', 'VIX3M']].max().max()

    host.set_ylim(data['SP500'].min()-5, data['SP500'].max()+5)
    par1.set_ylim(vix_min-2, vix_max+2)
    par2.set_ylim(0, pcr_max+0.1)
        
    host.set_xlabel("Date")
    host.set_ylabel("S&P 500 Closing Value")
    par1.set_ylabel("Volatility")
    par2.set_ylabel("Put-/Call-Ratio")

    color1 = plt.cm.viridis(0)
    color2 = plt.cm.viridis(0.5)
    color3 = plt.cm.viridis(.9)

    p1, = host.plot(data['Date'], data['SP500'],  color='#F56653', label="S&P 500", linewidth=2)
    
    p2, = par1.plot(data['Date'], data['VIX'], linestyle=(0, (1, 3)), color='#2A88AD', label="VIX", linewidth=3)
    p3, = par1.plot(data['Date'], data['VIX3M'], linestyle=(0, (1, 3)), color='#27B898', label="VIX3M", linewidth=3)

    p4, = par2.plot(data['Date'], data['TOTAL_PCR'], linestyle=(0, (5, 5)), color='#60E05F', label="TOTAL_PCR", linewidth=2)
    p5, = par2.plot(data['Date'], data['INDEX_PCR'], linestyle=(0, (5, 5)), color='#FFA97A', label="INDEX_PCR", linewidth=2)
    p6, = par2.plot(data['Date'], data['EQUITY_PCR'], linestyle=(0, (5, 5)), color='#DED449', label="EQUITY_PCR", linewidth=2)
    p7, = par2.plot(data['Date'], data['VIX_PCR'], linestyle=(0, (5, 5)), color='#E0A13A', label="VIX_PCR", linewidth=2)

    lns = [p1, p2, p3, p4, p5, p6, p7]
    l = par2.legend(handles=lns, loc='best', frameon=False, facecolor='white', framealpha=1)
    #l.get_frame().set_facecolor('white')

    # right, left, top, bottom
    par2.spines['right'].set_position(('outward', 60))

    # no x-ticks                 
    par2.xaxis.set_ticks(data['Date'])

    # Sometimes handy, same for xaxis
    #par2.yaxis.set_ticks_position('right')

    # Move "Velocity"-axis to the left
    # par2.spines['left'].set_position(('outward', 60))
    # par2.spines['left'].set_visible(True)
    # par2.yaxis.set_label_position('left')
    # par2.yaxis.set_ticks_position('left')

    host.yaxis.label.set_color(p1.get_color())
    par1.yaxis.label.set_color(p2.get_color())
    par2.yaxis.label.set_color(p4.get_color())

    # Adjust spacings w.r.t. figsize
    fig.tight_layout()
    # Alternatively: bbox_inches='tight' within the plt.savefig function 
    #                (overwrites figsize)

    # Best for professional typesetting, e.g. LaTeX
    plt.savefig( 'plots/' + dt.datetime.now().strftime("%Y-%m-%d") + "_data.png")

if __name__ == "__main__":
    # we assume this code is in /src while data is in /data. Since we do not want to assume a cwd we switch to src and than move one up
    path = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path)
    os.chdir(os.path.pardir)

    data = load_data_from_database('data/database.csv')
    plot_data(data)