# IMPORTS
import pandas as pd
import numpy as np
import math
import os.path
import time
from bitmex import bitmex
from binance.client import Client
from datetime import timedelta, datetime
from dateutil import parser
from tqdm import tqdm_notebook  # (Optional, used for progress-bars)
import pytz
from pprint import pprint

# API
bitmex_api_key = 'p-i7tFYXTSavUY0ANfYvO08O'  # Enter your own API-key here
# Enter your own API-secret here
bitmex_api_secret = 'LzrihY0DYXB9WtsPlxKkbv1ubpHbX-fJlj7l642xN44_efdO'
# binance_api_key = 'izozUzMHdh9T8mHRFgL38q749hMr7l2Oc3gfCvXd5Ng23Z94rMSFGIEKOqh31JP9'    #Enter your own API-key here
# binance_api_secret = 'wGsqQu6QjtJVib5Q7dih9srP4jLQJgiFic4yPWFHxNOhHkt6CJgQ1GyoOwNZBbeT' #Enter your own API-secret here
binance_api_key = os.environ.get('BINANCE_API')
binance_api_secret = os.environ.get('BINANCE_SECRET')

# CONSTANTS
binsizes = {"1m": 1, "5m": 5, "1h": 60, "1d": 1440}
batch_size = 750
bitmex_client = bitmex(test=False, api_key=bitmex_api_key,
                       api_secret=bitmex_api_secret)
binance_client = Client(api_key=binance_api_key, api_secret=binance_api_secret)
MAX_LIMIT = 500
START_TIME = "2014-12-26 11:00"
END_TIME = 1588978800000


# print(binance_client.get_all_tickers())
# FUNCTIONS
def minutes_of_new_data(symbol, kline_size, data, source, is_future=False, is_coin_margined_future=False):
    print('source = ', source)
    if len(data) > 0:
        old = parser.parse(data["timestamp"].iloc[-1])
    elif source == "binance":
        old = datetime.strptime('1 Jan 2017', '%d %b %Y')
    elif source == "bitmex":
        old = bitmex_client.Trade.Trade_getBucketed(
            symbol=symbol, binSize=kline_size, count=1, reverse=False).result()[0][0]['timestamp']
    if source == "binance":
        if is_future:
            new = pd.to_datetime(binance_client.futures_klines(
                symbol=symbol, interval=kline_size)[-1][0], unit='ms')
        elif is_coin_margined_future:
            new = pd.to_datetime(binance_client.coin_margined_futures_klines(
                symbol=symbol, interval=kline_size)[-1][0], unit='ms')
        else:
            new = pd.to_datetime(binance_client.get_klines(
                symbol=symbol, interval=kline_size)[-1][0], unit='ms')

    if source == "bitmex":
        new = bitmex_client.Trade.Trade_getBucketed(
            symbol=symbol, binSize=kline_size, count=1, reverse=True).result()[0][0]['timestamp']
    return old, new


def get_all_binance(symbol, kline_size, save=False, is_future=False, is_coin_margined_future=False, file_name=None):
    asset_id = ''
    if is_future:
        asset_id = 'future'
    elif is_coin_margined_future:
        asset_id = 'coin-margined-future'
    filename = 'binance-%s-%s-%sdata.csv' % (symbol,
                                             kline_size, asset_id)
    if file_name is not None:  # modify functionality for adding to varaious files
        filename = file_name
    if os.path.isfile(filename):
        data_df = pd.read_csv(filename)
        data_df.index = pd.to_datetime(data_df.index,utc=True)
        data_df = data_df.sort_values('timestamp')
    else: data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(symbol, kline_size, data_df, source = "binance",is_future=is_future,is_coin_margined_future=is_coin_margined_future)
    print(oldest_point, newest_point)
    oldest_point = oldest_point.replace(tzinfo=None)
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    if oldest_point == datetime.strptime('1 Jan 2017', '%d %b %Y'):
        print('Downloading all available %s data for %s. Be patient..!' %
              (kline_size, symbol))
    else:
        print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data.' % (
            delta_min, symbol, available_data, kline_size))
    
    print('calling get historical klines')
    print('is coin margined future: ', is_coin_margined_future)
    if is_future:
        klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime(
            "%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"), is_future=True)
    elif is_coin_margined_future:
        klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime(
            "%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"), is_coin_margined_future=True)
    else:
        klines = binance_client.get_historical_klines(symbol, kline_size, oldest_point.strftime(
            "%d %b %Y %H:%M:%S"), newest_point.strftime("%d %b %Y %H:%M:%S"))

    data = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close',
                                         'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
    if len(data_df) > 0:
        temp_df = pd.DataFrame(data)
        data_df = data_df.append(temp_df)
    else:
        data_df = data
    data_df.set_index('timestamp', inplace=True)
    # added to convert UTC -> EST for timestamps
    eastern = pytz.timezone('US/Eastern')
    data_df.index = data_df.index.tz_localize(pytz.utc).tz_convert(eastern)

    if save:
        data_df.to_csv(filename)
    print('All caught up..!')
    return data_df


def get_all_bitmex(symbol, kline_size, file_name, save=False):
    # filename = './data/%s-%s-data.csv' % (symbol, kline_size)
    filename = file_name
    if os.path.isfile(filename):
        print('found %s' % filename)
        data_df = pd.read_csv(filename)
    else:
        data_df = pd.DataFrame()
    oldest_point, newest_point = minutes_of_new_data(
        symbol, kline_size, data_df, source="bitmex")
    delta_min = (newest_point - oldest_point).total_seconds()/60
    available_data = math.ceil(delta_min/binsizes[kline_size])
    rounds = math.ceil(available_data / batch_size)
    if rounds > 0:
        print('Downloading %d minutes of new data available for %s, i.e. %d instances of %s data in %d rounds.' % (
            delta_min, symbol, available_data, kline_size, rounds))
        for round_num in tqdm_notebook(range(rounds)):
            time.sleep(1)
            new_time = (oldest_point + timedelta(minutes=round_num *
                                                 batch_size * binsizes[kline_size]))
            data = bitmex_client.Trade.Trade_getBucketed(
                symbol=symbol, binSize=kline_size, count=batch_size, startTime=new_time).result()[0]
            temp_df = pd.DataFrame(data)
            data_df = data_df.append(temp_df)
    data_df.set_index('timestamp', inplace=True)
    eastern = pytz.timezone('US/Eastern')
    data_df.index = data_df.index.tz_localize(pytz.utc).tz_convert(eastern)
    if save and rounds > 0:
        data_df.to_csv(filename)
    print('All caught up..!')
    return data_df


def get_funding_bitmex(symbol, file_name, save=False):
        # filename = './data/%s-%s-data.csv' % (symbol, kline_size)
    filename = file_name
    params = {
        'symbol': 'BTCUSDT',
        'startTime': START_TIME,
        'count': MAX_LIMIT,
        'reverse': False}

    funding_rates = []

    print(symbol)
    params['symbol'] = symbol
    params['startTime'] = pd.to_datetime(START_TIME)
    data_df = pd.DataFrame()
    while True:
        rates = bitmex_client.Funding.Funding_get(**params).result()[0]
        if rates is None or len(rates) == 0:
            break 
        funding_rates.extend(rates)
        params['startTime'] = rates[-1]['timestamp'] + timedelta(minutes=1)
        temp_df = pd.DataFrame(rates)
        data_df = data_df.append(temp_df)
    if save:
        data_df.to_csv(filename)
    print('All caught up..!')
    data_df.set_index('timestamp', inplace=True)
    eastern = pytz.timezone('US/Eastern')
    data_df.index = data_df.index.tz_localize(pytz.utc).tz_convert(eastern)
    return data_df
        # data =
        # data_df = pd.DataFrame(data)
        # eastern = pytz.timezone('US/Eastern')
        # data_df.index = data_df.index.tz_localize('GMT').tz_convert(eastern)


def get_symbols_bitmex():
    bitmex_client.Instrument.active()
