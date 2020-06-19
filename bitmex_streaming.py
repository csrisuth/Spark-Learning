'''
Bitmex Data Streaming for Kafka

This program is design to pull public tick data from Bitmex, crypo-currency future exchange, 
using websocket and send data to kafka topic.

Author: Charm Srisuthapan
Date: Jun 19, 2020
'''

import json
import logging
import time
import datetime
from bitmex_websocket import BitMEXWebsocket
from kafka import KafkaProducer

# Bitmex Endpoint
__TESTNET = False
if __TESTNET:
    __ENDPOINT = "https://testnet.bitmex.com/api/v1"
else:
    __ENDPOINT =  "wss://www.bitmex.com/realtime"

# API Key and Secret to connect to BitMEX, set to None if only need to access public data
__API_KEY = None
__API_SECRET = None

# Apache Kafka Setting
__HOSTNAME = 'localhost:9092'
__KAFKA_TOPIC = 'bitmex01'

# Get the nearest and second nearest expiration symbol of Future Contracts
def getSeries():
    currentMonth = datetime.datetime.now().month
    currentYear = datetime.datetime.now().year % 100

    if currentMonth in [1, 2, 3]:
        return ('H' + str(currentYear), 'M' + str(currentYear))
    elif currentMonth in [4, 5, 6]:
        return ('M' + str(currentYear), 'U' + str(currentYear))
    elif currentMonth in [7, 8, 9]:
        return ('U' + str(currentYear), 'Z' + str(currentYear))
    return ('Z' + str(currentYear), 'H' + str(currentYear + 1))

def getSymbolPair():
    series = getSeries()
    return ("XBTUSD", "XBT" + series[0], "XBT" + series[1])

# Check for update in trade history then send through socket and output to terminal
def getRecentTrade(ws, prev_row, logger, producer):
    try:
        recent_trades = ws.recent_trades()
        total_row = len(recent_trades)
        if total_row > prev_row:
            for row_index in range (prev_row, total_row):
                logger.info("Recent Trades : %s", recent_trades[row_index])
                producer.send(__KAFKA_TOPIC, recent_trades[row_index])
    except BaseException as e:
        print("Error on_data: %s" % str(e))
    return total_row


# Standard logger output for bitmex-ws
def setup_logger():
    
    # Prints logger info to terminal
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # add formatter to ch
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    return logger

def run(producer):
    logger = setup_logger()
    symbolSpot, symbolNear, symbolFar = getSymbolPair()

    #ws_spot = BitMEXWebsocket(endpoint=__ENDPOINT, symbol=symbolSpot, api_key=__API_KEY, api_secret=__API_SECRET)
    ws_near = BitMEXWebsocket(endpoint=__ENDPOINT, symbol=symbolNear, api_key=__API_KEY, api_secret=__API_SECRET)
    ws_far = BitMEXWebsocket(endpoint=__ENDPOINT, symbol=symbolFar, api_key=__API_KEY, api_secret=__API_SECRET)

    #prev_spot_count = 0
    prev_near_count = 0
    prev_far_count = 0

    while(ws_near.ws.sock.connected and ws_far.ws.sock.connected):
        #prev_spot_count = getRecentTrade(ws_spot, prev_spot_count, logger, producer)
        prev_near_count = getRecentTrade(ws_near, prev_near_count, logger, producer)
        prev_far_count = getRecentTrade(ws_far, prev_far_count, logger, producer)
        time.sleep(1)

if __name__ == "__main__":
    # Setup kafka producer properties
    producer = KafkaProducer(bootstrap_servers=[__HOSTNAME],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))  
    
    run(producer)
