""" Unit tests for teii.finance.timeseries module """


import datetime
import pytest
import filecmp
import numpy as np
import pandas as pd

from teii.finance import TimeSeriesFinanceClient
from teii.finance import FinanceClientInvalidAPIKey
from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClientParamError


def test_constructor_success(api_key_str,
                             mocked_response):
    TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)


def test_constructor_failure_invalid_data(api_key_str):
    with pytest.raises(FinanceClientInvalidData):
        TimeSeriesFinanceClient(["NOTICKER"], api_key_str)


def test_constructor_failure_invalid_api_key():
    with pytest.raises(FinanceClientInvalidAPIKey):
        TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"])


def test_daily_price_no_dates(api_key_str,
                              mocked_response,
                              pandas_series_IBM_prices):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.daily_price()

    for ps in l_ps:

        assert ps.count() == 5416   # 1999-11-01 to 2021-05-11 (5416 business days) sé que hay 5416

        assert ps.count() == pandas_series_IBM_prices.count()

        assert ps.equals(pandas_series_IBM_prices)


def test_daily_price_dates(api_key_str,
                           mocked_response,
                           pandas_series_IBM_prices_filtered):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.daily_price(datetime.date(year=2021, month=1, day=1),
                          datetime.date(year=2021, month=2, day=28),)

    for ps in l_ps:

        assert ps.count() == 38   # 2021-01-04 to 2021-02-26 (38 business days)

        assert ps.count() == pandas_series_IBM_prices_filtered.count()

        assert ps.equals(pandas_series_IBM_prices_filtered)


def test_to_pandas_success(api_key_str,
                           mocked_response,
                           pandas_series_IBM):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.to_pandas()

    for ps in l_ps:

        assert len(ps.index) == 5416

        assert len(ps.index) == len(pandas_series_IBM.index)

        assert ps.equals(pandas_series_IBM)


def test_to_csv_success(api_key_str,
                        mocked_response,
                        path_csv,
                        sandbox_root_path):
    fc = TimeSeriesFinanceClient(["IBM"], api_key_str)

    fc.to_csv("test.csv")

    assert filecmp.cmp("test.csv", path_csv, shallow=False)


def test_daily_volume_no_dates(api_key_str,
                               mocked_response,
                               pandas_series_IBM_volume):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.daily_volume()

    for ps in l_ps:

        assert ps.count() == 5416   # 1999-11-01 to 2021-05-11 (5416 business days) sé que hay 5416

        assert ps.count() == pandas_series_IBM_volume.count()

        assert ps.equals(pandas_series_IBM_volume)


def test_daily_volume_dates(api_key_str,
                            mocked_response,
                            pandas_series_IBM_volume_filtered):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.daily_volume(datetime.date(year=2021, month=1, day=1),
                           datetime.date(year=2021, month=2, day=28),)

    for ps in l_ps:

        assert ps.count() == 38   # 2021-01-04 to 2021-02-26 (38 business days)

        assert ps.count() == pandas_series_IBM_volume_filtered.count()

        assert ps.equals(pandas_series_IBM_volume_filtered)


def test_daily_volume_dates_failure(api_key_str,
                                    mocked_response):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    with pytest.raises(FinanceClientParamError):
        fc.daily_volume(datetime.date(year=2021, month=1, day=1),
                        datetime.date(year=2020, month=2, day=28),)


def test_yearly_dividends_no_dates(api_key_str,
                                   mocked_response,
                                   pandas_series_IBM_dividend):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.yearly_dividends()

    for ps in l_ps:

        assert ps.count() == 23   # 1999 to 2021 (23 years)

        assert ps.count() == pandas_series_IBM_dividend.count()

        assert np.allclose(ps, pandas_series_IBM_dividend)


def test_yearly_dividends_dates(api_key_str,
                                mocked_response,
                                pandas_series_IBM_dividend_filtered):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.yearly_dividends(2010, 2021)

    for ps in l_ps:

        assert ps.count() == 12  # 2010 to 2021 (12 years)

        assert ps.count() == pandas_series_IBM_dividend_filtered.count()

        assert np.allclose(ps, pandas_series_IBM_dividend_filtered)


def test_yearly_dividens_dates_failure(api_key_str,
                                       mocked_response):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    with pytest.raises(FinanceClientParamError):
        fc.yearly_dividends(datetime.date(year=2021, month=1, day=1),
                            datetime.date(year=2020, month=2, day=28))


def test_yearly_dividends_quarter_no_dates(api_key_str,
                                           mocked_response,
                                           pandas_series_IBM_dividend_quarter):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.yearly_dividends_per_quarter()

    for ps in l_ps:

        assert ps.count() == 87   # 1999-10 to 2021-04 (87 quarters)

        assert ps.count() == pandas_series_IBM_dividend_quarter.count()

        assert np.allclose(ps, pandas_series_IBM_dividend_quarter)


def test_yearly_dividends_quarter_dates(api_key_str,
                                        mocked_response,
                                        pandas_series_IBM_dividend_quarter_filtered):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_ps = fc.yearly_dividends_per_quarter(1999, 2007)

    for ps in l_ps:

        assert ps.count() == 33   # 1999-10 to 2007 (33 quarters)

        assert ps.count() == pandas_series_IBM_dividend_quarter_filtered.count()

        assert np.allclose(ps, pandas_series_IBM_dividend_quarter_filtered)


def test_yearly_dividens_quarter_dates_failure(api_key_str,
                                               mocked_response):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    with pytest.raises(FinanceClientParamError):
        fc.yearly_dividends_per_quarter(datetime.date(year=2021, month=1, day=1),
                                        datetime.date(year=2020, month=2, day=28))


def test_highest_daily_variation(api_key_str,
                                 mocked_response):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_t_fc = fc.highest_daily_variation()

    for t_fc in l_t_fc:

        dt = datetime.date(year=2020, month=3, day=16)
        ts = pd.to_datetime(str(dt))
        t = (ts, 107.41, 95.0, 12.409999999999997)

        assert t == t_fc


def test_highest_monthly_mean_variation(api_key_str,
                                        mocked_response):
    fc = TimeSeriesFinanceClient(["IBM", "IBM", "IBM", "IBM"], api_key_str)

    l_t_fc = fc.highest_monthly_mean_variation()

    for t_fc in l_t_fc:

        dt = datetime.date(year=2020, month=3, day=1)
        ts = pd.to_datetime(str(dt))
        t = (ts, 6.833636363636362)

        assert t == t_fc
