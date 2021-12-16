#!/usr/bin/env python3
from sqlite3.dbapi2 import Error
from websocket import create_connection
import json
import time
import threading
from time import sleep
from function_ftx import transform_market_data,generate_query,insert_data
import sqlite3

def process_ws_data(data,market='BTC-PERP',trade_data='trade',exchange='FTX'):
    filt_mkt_data_ws=[]
    for i in range(1,len(data)):
        mkt_data_ws=json.loads(data[i])
        mkt_data_ws=mkt_data_ws["data"]
        mkt_data=transform_market_data(mkt_data_ws,market,trade_data,exchange)
        query=generate_query(mkt_data[0],trade_data)
        insert_data(query,conn)
ws = create_connection("wss://ftx.com/ws/")
db_file = r"tradeblock_data.db"
conn = sqlite3.connect(db_file)
x={'op': 'subscribe', 'channel': 'trades', 'market': 'BTC-PERP'}
x=json.dumps(x)
ws.send(x)
print("Sent")
print("Receiving...")
data=[]
while(1):
    result =  ws.recv()
    data.append(result)
    print("Received")
    process_ws_data(data)
    time.sleep(0.5)
ws.close()


