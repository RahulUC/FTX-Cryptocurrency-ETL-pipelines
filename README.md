# FTX-Cryptocurrency-ETL-pipelines
Using REST API and Websocket to store crypto trade executions and per minute,hour and day aggregated data with data validation and logs
To run the code:-
1)To install all the required libraries.

pip install -r requirements.txt

2)Run trade_history_data.py - which creates the DB, Trigger and insert trade and history data through REST API

./trade_history_data

3)Run websocket_trade.py- which inserts WebSocket trade data(Note:- Currently, Data is stored at regular intervals so not all data points are captured)

./websocket_trade.py

4)Run create_agg_hist_data_ws.py- Aggregate  the trade data and insert it in the history table and store the delta in the log table

./create_agg_hist_data_ws.py

5)A tradeblock_data.db file will be created with the following tables:
a)data_diff_log-for storing deltas
b)history_trade- For aggregated data and open Interest at each interval
c)trade-for storing trade executions
