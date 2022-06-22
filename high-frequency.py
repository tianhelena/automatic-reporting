import requests
import pandas as pd 
import lxml
import json
import ast
import datetime
import imf_datatools
from imf_datatools import haver_utilities
from datetime import datetime
from pandas import Series,DataFrame
import matplotlib.pyplot as plt
from matplotlib.figure import Figure
from matplotlib.ticker import (MultipleLocator, FormatStrFormatter,AutoMinorLocator)
import matplotlib.dates as mdates
import os 
import pickle 

def get_data(var):
    df = haver_utilities . get_haver_data ([s for s in var])
    df.columns = [col.split("@")[0] for col in df.columns] 
    df_meta=haver_utilities .get_haver_metadata([s for s in var], debug=False)
    df_meta['code']=df_meta['code'].str.upper()
    df_meta[['Main','Minor']]=df_meta["descriptor"].str.split("(", expand=True)
    df_meta['Main'] = df_meta['Main'].str.replace(r'Saudi Arabia:', '').str.replace(r'Saudi Arabia', '')
    descriptor=df_meta[['code','Main','Minor']].set_index('code').to_dict('index')
    return df,descriptor
    
def filter(df,column, value): 
    return df[df[column]==value]

def footnote(text):
    axes[1][1].annotate(text,  # Your string

            # The point that we'll place the text in relation to 
            xy=(-1.3, 0), 
            # Interpret the x as axes coords, and the y as figure coords
            xycoords=('axes fraction', 'figure fraction'),

            # The distance from the point that the text will be at
            xytext=(1, 1),  
            # Interpret `xytext` as an offset in points...
            textcoords='offset points',

            # Any other text parameters we'd like
            size=10, ha='left', va='bottom')
    

def plot_one(df, chart_title, ax,subtitle):
    ax.plot(df)
    ax.set_title(chart_title,loc='left',fontweight="bold",fontsize=12)
    ax.set_title(subtitle,fontsize=8, loc='right')
    if len(df.columns)>1:
        ax.legend([metadata[var]['Main'] for var  in df.columns],fontsize='x-small')
    
def formatplot(axes):
    for ax_row in axes:
       for ax in ax_row: 
           ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3)) 
           ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
##shipping
df_sh= pd.read_stata('000-daily-trade-estimates.dta')   ##daily
df_sh=filter(df_sh,'country','SA')
#df_sh=filter(df_sh,'VESSEL_TYPE_COARSE','Total')
df_sh=df_sh[['date','imp_mtc_ma']]
df_sh=df_sh.set_index('date')
df_sh=df_sh.rolling(7).sum()
df_sy=((df_sh/df_sh.shift(365))-1)*100
df_sy=df_sy.loc['2020-01-01':]


##Google mobility data 
with open('mobility_code.txt', "rb") as fp:   
        var_list= pickle.load(fp)
df_g,metadata=get_data(var_list)
df_g=df_g.rolling(window=7).mean()
for col in df_g.columns: 
    metadata[col]['Main']=metadata[col]['Main'].replace(r' Change in Visits Relative to Baseline:', '')   ###for improvement 
    
##Tomtom  Jan3-Feb 6=1, 7-day rolling average
df_T= pd.read_csv('Tomtom_live.csv')  ##daily 
df_T=df_T.rename(columns=df_T.iloc[0]) 
df_T=df_T.drop([0, 1])
df_T=df_T.rename(columns={'City':'Date'})
df_T['Date'] = df_T['Date'].astype(str).apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
df_T=df_T.set_index('Date')
df_T = df_T.astype(float)
df_T=df_T.rolling(window=7).mean()
#df_t=(df_T/(df_T.loc['2020-01-10':'2020-02-06'].mean(axis=0))-1)*100
#df_T=df_T.loc['2020-02-17':]

metadata.update({'SAU_jeddah': {'Main': 'Jeddah'}})
metadata.update({'SAU_riyadh': {'Main': 'Riyadh'}})
    
##Fight data       ##daily
import pyodbc
import pandas as pd

conn_args = dict()
conn_args['driver']='{SQL Server Native Client 11.0}'
conn_args['server']=' prdecossql2012,5876'
conn_args['database']='STA_Covid19'
conn_args['Trusted_connection']='yes'

con = pyodbc.connect(**conn_args)
cursor = con.cursor()

# Example query

sql = """SELECT TOP (1000000) *
         FROM [STA_Covid19].[dbo].[FlightRadars]"""
rows = cursor.execute(sql).fetchall()
rows = [list(r) for r in rows]
df_f = pd.DataFrame(rows, columns=["Id", "Country", "Date", "Indicator", "Value"])



df_f=df_f[df_f['Country']=='Saudi Arabia']   
df_f=df_f.drop(columns=['Country', 'Id'])

df_f['Date'] = df_f['Date'].astype(str).apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
df_f=df_f.sort_values(by=['Date'])
#df_f=df_f.set_index('Date')
df_f=df_f[df_f['Indicator']!='Domestic']

df_f=df_f.set_index(['Date','Indicator']).unstack(level=1)
df_f.columns=df_f.columns.droplevel()

df_f=df_f.loc['2020-01-01 00:00:00':]

metadata.update({'International Arrivals': {'Main': 'International Arrivals'}})
metadata.update({'International Departures': {'Main': 'International Departures'}})


##Plottong 
fig, axes = plt.subplots(2,2, figsize=(12,8))
fig.suptitle('Daily data',size=16,fontweight="bold",x=0.18)
formatplot(axes)

plot_one(df_f,'Flights', axes[0][0],'(Number of flights)')
plot_one(df_sy,'Imports', axes[0][1],'(Yoy change,30-day rolling average)')
plot_one(df_g,'Mobility', axes[1][0],'(Change from baseline:Jan3-Feb6, 7-day rolling average)')
plot_one(df_T,'Traffic', axes[1][1],'')


plt.savefig('P1.pdf')





#axes[1][0].plot(df_g)
#axes[1][0].set_title('Mobility',loc='left',fontweight="bold",fontsize=14)
#axes[1][0].set_title('(Change from baseline:Jan3-Feb6, 7-day rolling average)',fontsize=8, loc='right')
#axes[1][0].legend(df_g.columns,bbox_to_anchor=(0.6,0.7),fontsize='x-small')
#axes[1][0].xaxis.set_major_locator(mdates.MonthLocator(interval=1)) 
#axes[1][0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
#
#axes[1][1].plot(df_T)
#axes[1][1].set_title('Traffic',loc='left',fontweight="bold",fontsize=14)
#axes[1][1].set_title('(Change from baseline:Jan3-Feb6, 7-day rolling average)',fontsize=8, loc='right')
#axes[1][1].legend(df_T.columns)
#axes[1][1].legend(df_T.columns, bbox_to_anchor=(0.25,0.35),fontsize='x-small')
#axes[1][1].xaxis.set_major_locator(mdates.MonthLocator(interval=1)) 
#axes[1][1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))


