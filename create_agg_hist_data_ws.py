#!/usr/bin/env python3
import sqlite3
from function_ftx import run_ddl_query
import time
db_file = r"tradeblock_data.db"
intervals=[60,60*60,60*60*24]
while(1):
    conn = sqlite3.connect(db_file)
    for interval in intervals:
        interval=str(interval)
        query="""
        with cte1 AS(
select date,market,"""+interval+""",exchange,datetime(strftime('%s','now')-strftime('%s','now')%60-"""+interval+""",'unixepoch') startTime,(strftime('%s','now')-strftime('%s','now')%60-"""+interval+""")*1000 as time,first_value(price) over(order by epoch_time asc) as open,max(price) over() high,min(price) over() low,last_value(price) over(order by epoch_time asc) close,sum(size) over() volume,sum(Case when side='buy' then 1 else -1 end) as open_interest from trade
where (epoch_time/(1000*1000))>=strftime('%s','now')-strftime('%s','now')%60-"""+interval+"""
group by date,market,exchange)
        INSERT into history_trade SELECT * from cte1 where 1=1 on CONFLICT(market,interval,exchange,startTime) 
DO UPDATE set open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,volume=excluded.volume,open_interest=excluded.open_interest;"""
        print(interval)
        run_ddl_query(query,conn)
    conn.close()
    time.sleep(min(intervals))
