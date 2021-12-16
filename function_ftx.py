#!/usr/bin/env python3
import sqlite3
from sqlite3 import Error
import requests
from datetime import datetime

def extract_data_api(api,market):
        req = requests.get(api)
        market_data = req.json()
        return market_data
def run_ddl_query(create_table_sql,conn):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def insert_data(query,conn):
    try:
        c = conn.cursor()
        c.execute(query)
        conn.commit()
    except Error as e:
        print(e)

def transform_market_data(market_data,market,dtype,exchange,interval=60):
    filt_data=[]
    epoch = datetime(1970, 1, 1)
    if(dtype=='history'):
        for i in market_data:
            startTime=i['startTime'].replace('T',' ').split('+')[0]
            date=i['startTime'].split('T')[0]
            time=i['time']
            open=i['open']
            high=i['high']
            low=i['low']
            close=i['close']
            volume=i['volume']
            oi=None
            filt_data.append([date,market,interval,startTime,time,open,high,low,close,volume,exchange,oi])
        return filt_data
    elif(dtype=='trade'):
        p = '%Y-%m-%dT%H:%M:%S.%f'
        for i in market_data:
            date=i['time'].split('T')[0]
            trade_id=i['id']
            price=i['price']
            size=i['size']
            side=i['side']
            liquidation=i['liquidation']
            time=i['time'].split('+')[0].replace('T',' ')[:19]
            epoch_time=int((datetime.strptime(i['time'].split('+')[0], p) - epoch).total_seconds()*1000000)
            filt_data.append([date,trade_id,price,size,side,liquidation,time,epoch_time,market,exchange])
    else:
        print('Error:Type of data not found')
    return filt_data
    

def generate_query(data,dtype):
    if(dtype=='history'):
        date=str(data[0])
        market=str(data[1])
        interval=str(data[2])
        startTime=str(data[3])
        time=str(data[4])
        open=str(data[5])
        high=str(data[6])
        low=str(data[7])
        close=str(data[8])
        volume=str(data[9])
        exchange=str(data[10])
        open_interest='Null'
        query="""Insert into history_trade values('"""+date+"""','"""+market+"""',"""+interval+""",'"""+exchange+"""','"""+startTime+"""',"""+time+""","""+open+""","""+high+""","""+low+""","""+close+""","""+volume+""","""+open_interest+""")"""
    elif(dtype=='trade'):
        date=str(data[0])
        trade_id=str(data[1])
        price=str(data[2])
        size=str(data[3])
        side=str(data[4])
        liquidation=str(data[5])
        time=str(data[6])
        epoch_time=str(data[7])
        market=str(data[8])
        exchange=str(data[9])
        query="""Insert into trade values('"""+date+"""',"""+trade_id+""",'"""+market+"""','"""+exchange+"""',"""+price+""","""+size+""",'"""+side+"""',"""+liquidation+""",'"""+time+"""',"""+epoch_time+""") on CONFLICT(date,trade_id,market,exchange) DO update set price="""+price+""",size="""+size+""",side='"""+side+"""',liquidation="""+liquidation+""",time='"""+time+"""',epoch_time="""+epoch_time+""";"""
        return query
    else:
        print('Error:Type of data not found')
    return query