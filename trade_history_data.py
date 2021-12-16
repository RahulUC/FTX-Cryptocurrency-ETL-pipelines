#!/usr/bin/env python3
from sqlite3.dbapi2 import Error
import json
import time
from function_ftx import transform_market_data,generate_query,insert_data,run_ddl_query,extract_data_api
import sqlite3
intervals=[60,60*60,60*60*24]
markets=['BTC-PERP']
db_file = r"tradeblock_data.db"
history_data='history'
trade_data='trade'
exchanges=['FTX']
conn = sqlite3.connect(db_file)
#crete tables
drop_trade="drop table IF EXISTS trade;"
drop_trade_hist="drop table IF EXISTS history_trade;"
create_trade_history = """ create table IF NOT EXISTS history_trade (date date,market varchar(20),interval integer,exchange varchar(20),startTime datetime,time time,open Decimal(10,8) not null,high  Decimal(10,8) not null,low  Decimal(10,8) not null,close  Decimal(10,8) not null,volume Decimal(10,8) not null,open_interest int,CONSTRAINT PK_hist PRIMARY KEY (market,interval,exchange,startTime)); """
create_trade_table = """ create table IF NOT EXISTS trade (date date,trade_id BIGINT, market varchar(20),exchange varchar(20),price DECIMAL(10,8),size DECIMAL(10,8),side varchar(20),liquidation BOOLEAN,time DATETIME,epoch_time bigint, CONSTRAINT PK_trade PRIMARY KEY (date,trade_id,market,exchange)); """
#create_trade_table = """ create table IF NOT EXISTS trade (date date,trade_id BIGINT, market varchar(20),exchange varchar(20),price DECIMAL(10,8),size DECIMAL(10,8),side varchar(20),liquidation BOOLEAN,time DATETIME,epoch_time bigint;"""
create_log_table="create table IF NOT EXISTS  data_diff_log(date date,market varchar(20),interval integer,exchange varchar(20),startTime datetime,open_diff Decimal(10,8) not null,high_diff  Decimal(10,8) not null,low_diff  Decimal(10,8) not null,close_diff  Decimal(10,8) not null,volume_diff Decimal(10,8) not null);"
log_trigger="""CREATE TRIGGER `trig_data_diff_log` BEFORE UPDATE ON `history_trade`
For EACH ROW when (new.open-old.open!=0 or new.high-old.high!=0 or new.low-old.low!=0 or new.close-old.close!=0 or new.volume-old.volume!=0)
BEGIN
INSERT INTO data_diff_log (date,market,interval,exchange,startTime,open_diff,high_diff,low_diff,close_diff,volume_diff)
VALUES(new.date,new.market,new.interval,new.exchange,new.startTime,new.open-old.open,new.high-old.high,new.low-old.low,new.close-old.close,new.volume-old.volume); 
END"""
run_ddl_query(drop_trade,conn);
run_ddl_query(drop_trade_hist,conn)
run_ddl_query(create_trade_history,conn)
run_ddl_query(create_trade_table,conn)
run_ddl_query(create_log_table,conn)
insert_data(log_trigger,conn)
conn.close()
while(1):
    conn = sqlite3.connect(db_file)
    for exchange in exchanges:
        for market in markets:
            trade_api='https://ftx.com/api/markets/'+market+'/trades'
            #process Trade data
            trade_market_data=extract_data_api(trade_api,market)
            trade_market_data=trade_market_data["result"]
            filt_mkt_data=transform_market_data(trade_market_data,market,trade_data,exchange)
            for i in range(len(filt_mkt_data)):
                    query=generate_query(filt_mkt_data[i],trade_data)
                    insert_data(query,conn)

            #process History data
            for interval in intervals:
                history_api='https://ftx.com/api/markets/'+market+'/candles?resolution='+str(interval)
                hist_market_data=extract_data_api(history_api,market)
                hist_market_data=hist_market_data["result"]
                filt_hist_data=transform_market_data(hist_market_data,market,history_data,exchange,interval)
                for i in range(len(filt_hist_data)):
                    query=generate_query(filt_hist_data[i],history_data)
                    insert_data(query,conn)   
                print(market,interval)            
    conn.close()
    time.sleep(min(intervals))