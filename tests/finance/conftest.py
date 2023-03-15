""" Unit tests for teii.finance subpackage """


import json
import pandas as pd
import unittest.mock as mock
import teii.finance.finance

from importlib import resources
from pytest import fixture


@fixture(scope='session')
def api_key_str():
    return ("nokey")


@fixture(scope='package')
def mocked_response():
    def mocked_get(url):
        response = mock.Mock()
        response.status_code = 200
        if 'IBM' in url or 'NOTICKER' in url:
            json_filename = 'TIME_SERIES_DAILY_ADJUSTED.IBM.json'
        else:
            raise ValueError('Ticker no soportado')
        with resources.open_text('teii.finance.data', json_filename) as json_fid:
            json_data = json.load(json_fid)
        response.json.return_value = json_data
        return response

    requests = mock.Mock()
    requests.get.side_effect = mocked_get

    teii.finance.finance.requests = requests


@fixture(scope='package')
def pandas_series_IBM_prices():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.prices.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['close']
    return ds


@fixture(scope='package')
def pandas_series_IBM_prices_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.prices.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['close']
    return ds


@fixture(scope='package')
def pandas_series_IBM():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
    return df


@fixture(scope='package')
def path_csv():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.unfiltered.csv') as path2csv:
        return path2csv


@fixture(scope='package')
def pandas_series_IBM_volume():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.volume.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['volume']
    return ds


@fixture(scope='package')
def pandas_series_IBM_volume_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.volume.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['volume']
    return ds


@fixture(scope='package')
def pandas_series_IBM_dividend():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.yearly_dividends.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['dividend']
    return ds


@fixture(scope='package')
def pandas_series_IBM_dividend_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.yearly_dividends.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['dividend']
    return ds


@fixture(scope='package')
def pandas_series_IBM_dividend_quarter():
    with resources.path('teii.finance.data',
                        'TIME_SERIES_DAILY_ADJUSTED.IBM.yearly_dividends_quarter.unfiltered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['dividend']
    return ds


@fixture(scope='package')
def pandas_series_IBM_dividend_quarter_filtered():
    with resources.path('teii.finance.data', 'TIME_SERIES_DAILY_ADJUSTED.IBM.yearly_dividends_quarter.filtered.csv') as path2csv:
        df = pd.read_csv(path2csv, index_col=0, parse_dates=True)
        ds = df['dividend']
    return ds
