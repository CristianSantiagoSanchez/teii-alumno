""" Time Series Finance Client classes """


import datetime as dt
import logging
import pandas as pd
import typing

from typing import Optional, Union

from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClientParamError
from teii.finance import FinanceClient


class TimeSeriesFinanceClient(FinanceClient):
    """ Wrapper around the AlphaVantage API for Time Series Daily Adjusted.

        Source:
            https://www.alphavantage.co/documentation/ (TIME_SERIES_DAILY_ADJUSTED)

        Attributes:
        ----------
        _data_field2name_type: dict[str, dict[str, str]]
            Dictionary to define the name and type columns.

        Mothods:
        --------
        _build_data_frame()
            Build Panda's DataFrame and format data.
        _build_base_query_url_params(self, ticker)
            Return base query URL parameters.
        _build_query_data_key()
            Return data query key
        _validate_query_data(ticker)
            Validate the query data
        daily_price(from_date = None, to_date = None,)
            Return daily close price from 'from_date' to 'to_date'.
        daily_volume(from_date = None, to_date = None)
            Return daily volume from 'from_date' to 'to_date'.
        yearly_dividends(from_year = None, to_year = None)
            Return yearly dividends from 'from_year' to 'to_year'.
        yearly_dividends_per_quarter(from_year = None, to_year = None)
            Return yearly dividends per quarter from 'from_year' to 'to_year'.
        highest_daily_variation()
            Return the list of the highest daily variation
        highest_monthly_mean_variation()
            Return the list of the highest monthly variation
    """

    _data_field2name_type = {
            "1. open":                  ("open",     "float"),
            "2. high":                  ("high",     "float"),
            "3. low":                   ("low",      "float"),
            "4. close":                 ("close",    "float"),
            "5. adjusted close":        ("aclose",   "float"),
            "6. volume":                ("volume",   "int"),
            "7. dividend amount":       ("dividend", "float"),
            "8. split coefficient":     ("splitc",   "int"),
        }

    def __init__(self, ticker: list,
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.INFO) -> None:
        """ TimeSeriesFinanceClient constructor.
        Parameters:
        ----------
        ticker: typing.List[str]
            Contains the list of tickers
        api_key: str, optional
            key to search the URLs (default is None)
        logging_level: Union[int, str]
            defines the level of the logger (default is logging.INFO)
        """

        super().__init__(ticker, api_key, logging_level)
        self._logger.info("Construyendo TimeSeriesFinanceClient")
        self._build_data_frame()

    def _build_data_frame(self) -> None:
        """ Build Panda's DataFrame and format data.
        Raises:
        ------
        FinanceClientInvalidData
            Data json not specified,not found in axis or not understood
        """

        #   Tarea 3
        #   Comprueba que no se produce ningún error y genera excepción
        #   'FinanceClientInvalidData' en caso de error (hay un ejemplo en línea 86)
        for i, tick in enumerate(self._ticker):
            # Build Panda's data frame
            try:
                data_frame = pd.DataFrame.from_dict(self._json_data[i], orient='index', dtype=float)
            except TypeError as t:
                self._logger.error(f"{t}", exc_info=False)
                raise FinanceClientInvalidData("Data json not specified") from t

            # Rename data fields
            try:
                data_frame = data_frame.rename(columns={key: name_type[0]
                                                        for key, name_type in self._data_field2name_type.items()})
            except KeyError as k:
                self._logger.error(f"{k}", exc_info=False)
                raise FinanceClientInvalidData("Not found in axis") from k

            # Set data field types
            try:
                data_frame = data_frame.astype(dtype={name_type[0]: name_type[1]
                                               for key, name_type in self._data_field2name_type.items()})
            except TypeError as t:
                self._logger.error(f"{t}", exc_info=False)
                raise FinanceClientInvalidData("Data type not understood") from t

            # Set index type
            data_frame.index = data_frame.index.astype("datetime64[ns]")

            # Sort data
            self._data_frame.append(data_frame.sort_index(ascending=True))
            self._logger.info(f"Creado data frame del ticker '{tick}'")

    def _build_base_query_url_params(self, ticker: str) -> str:
        """ Return base query URL parameters.

        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=TICKER&outputsize=full&apikey=API_KEY&data_type=json

        Parameters:
        ----------
        ticker: str
            Contain the ticker

        Return:
        ------
        str
            Contains te base query url parameters
        """
        self._logger.info("Obteniendo parametros de URL de peticion")

        return f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={ticker}&outputsize=full&apikey={self._api_key}"

    def _build_query_data_key(cls) -> str:
        """ Return data query key.
        Return:
        ------
        str
            Time Series (Daily)
        """

        return "Time Series (Daily)"

    def _validate_query_data(self, ticker: str) -> None:
        """ Validate query data.
        Parameteres:
        ------------
        ticker: str
            Contains the ticker

        Raises:
        -------
        FinanceClientInvalidData
            Metadata field '2. Symbol' not found
        """
        self._logger.info("Validando datos de peticion")
        try:
            assert self._json_metadata[-1]["2. Symbol"] == ticker
        except Exception as e:
            self._logger.error(f"{e}", exc_info=False)
            raise FinanceClientInvalidData("Metadata field '2. Symbol' not found") from e
        else:
            self._logger.info(f"Metadata key '2. Symbol' = '{self._ticker[-1]}' found")

    def daily_price(self,
                    from_date: Optional[dt.date] = None,
                    to_date: Optional[dt.date] = None) -> typing.List[pd.Series]:
        """ Return daily close price from 'from_date' to 'to_date'.
        Parameters:
        ----------
        from_date: datetime.date
            The initial date
        to_date: datetime.date
            The final date

        Return:
        ------
        typing.List[pd.Series]
            List of panda's series.

        Raises:
        ------
        FinanceClientParamError
            Dates are incorrect
        """
        self._logger.info(f"Obteniendo precio diario de los tickers {self._ticker}")
        assert self._data_frame is not None

        ser: typing.List[pd.Series] = []
        for df in self._data_frame:
            series = df['close']

            #   Comprueba que from_date <= to_date y genera excepción
            #   'FinanceClientParamError' en caso de error (hay que crear dicha excepción)
            if from_date is not None and to_date is not None:
                try:
                    assert from_date <= to_date  # type: ignore
                except Exception as e:
                    self._logger.error(f"{e}", exc_info=False)
                    raise FinanceClientParamError("Dates are incorrect") from e

            if from_date is not None and to_date is not None:
                series = series.loc[from_date:to_date]   # type: ignore

            ser.append(series)

        return ser

    def daily_volume(self,
                     from_date: Optional[dt.date] = None,
                     to_date: Optional[dt.date] = None) -> typing.List[pd.Series]:
        """ Return daily volume from 'from_date' to 'to_date'.
        Parameters:
        ----------
        from_date: datetime.date
            The initial date
        to_date: datetime.date
            The final date

        Return:
        ------
        typing.List[pd.Series]
            List of panda's series.

        Raises:
        ------
        FinanceClientParamError
            Dates are incorrect
        """
        self._logger.info(f"Obteniendo volumen diario de los tickers {self._ticker}")
        assert self._data_frame is not None

        ser: typing.List[pd.Series] = []
        for df in self._data_frame:
            series = df['volume']

            #   Comprueba que from_date <= to_date y genera excepción
            #   'FinanceClientParamError' en caso de error

            if from_date is not None and to_date is not None:
                try:
                    assert from_date <= to_date  # type: ignore
                except Exception as e:
                    self._logger.error(f"{e}", exc_info=False)
                    raise FinanceClientParamError("Dates are incorrect") from e

            if from_date is not None and to_date is not None:
                series = series.loc[from_date:to_date]  # type: ignore

            ser.append(series)

        return ser

    def yearly_dividends(self,
                         from_year: Optional[int] = None,
                         to_year: Optional[int] = None) -> typing.List[pd.Series]:
        """ Return yearly dividends from 'from_year' to 'to_year'.
        Parameters:
        ----------
        from_year: int
            The initial year
        to_year: int
            The final year

        Return:
        ------
        typing.List[pd.Series]
            List of panda's series.

        Raises:
        ------
        FinanceClientParamError
            Dates are incorrect
        """
        self._logger.info(f"Obteniendo dividendos anuales de los tickers {self._ticker}")
        assert self._data_frame is not None

        ser: typing.List[pd.Series] = []
        for df in self._data_frame:
            series = df['dividend'].groupby(pd.Grouper(level=0, freq='1YS')).sum()
            series.index = series.index.strftime('%Y')

            #   Comprueba que from_year <= to_year y genera excepción
            #   'FinanceClientParamError' en caso de error

            if from_year is not None and to_year is not None:
                try:
                    assert from_year <= to_year  # type: ignore
                except Exception as e:
                    self._logger.error(f"{e}", exc_info=False)
                    raise FinanceClientParamError("Dates are incorrect") from e

            if from_year is not None and to_year is not None:
                series = series.loc[str(from_year):str(to_year)]  # type: ignore

            ser.append(series)

        return ser

    def yearly_dividends_per_quarter(self,
                                     from_year: Optional[int] = None,
                                     to_year: Optional[int] = None) -> typing.List[pd.Series]:
        """ Return yearly dividends per quarter from 'from_year' to 'to_year'.
        Parameters:
        ----------
        from_year: int
            The initial year
        to_year: int
            The final year

        Return:
        ------
        typing.List[pd.Series]
            List of panda's series.

        Raises:
        ------
        FinanceClientParamError
            Dates are incorrect
        """
        self._logger.info(f"Obteniendo dividendos trimestrales de los tickers {self._ticker}")
        assert self._data_frame is not None

        ser: typing.List[pd.Series] = []
        for df in self._data_frame:
            series = df['dividend'].resample('QS').sum()

            #   Comprueba que from_year <= to_year y genera excepción
            #   'FinanceClientParamError' en caso de error

            if from_year is not None and to_year is not None:
                try:
                    assert from_year <= to_year  # type: ignore
                except Exception as e:
                    self._logger.error(f"{e}", exc_info=False)
                    raise FinanceClientParamError("Dates are incorrect") from e

            if from_year is not None and to_year is not None:
                series = series.loc[str(from_year):str(to_year)]  # type: ignore

            ser.append(series)

        return ser

    def highest_daily_variation(self) -> typing.List[typing.Tuple]:
        """ Return the list of the highest daily variation
        Return:
        ------
        typing.List[typing.Tuple]
            List of tuple that contains the highest daily variation
        """
        assert self._data_frame is not None
        self._logger.info(f"Obteniendo la maxima variacion diaria de los tickers {self._ticker}")
        tupla: typing.List[typing.Tuple] = []
        for df in self._data_frame:
            series = df[['high', 'low']].copy()

            series['high-low'] = series['high'] - series['low']

            series_highest = series.sort_values('high-low', ascending=False).head(1)

            i = series_highest.index.values[0]

            v = series_highest.values

            tup = (pd.to_datetime(str(i)), v[0][0], v[0][1], v[0][2])

            tupla.append(tup)

        return tupla

    def highest_monthly_mean_variation(self) -> typing.List[typing.Tuple]:
        """ Return the list of the highest monthly variation
        Return:
        ------
        typing.List[typing.Tuple]
            List of tuple that contains the highest monthly mean variation
        """
        assert self._data_frame is not None
        self._logger.info(f"Obteniendo la maxima variación media mensual de los tickers {self._ticker}")
        tupla: typing.List[typing.Tuple] = []
        for df in self._data_frame:
            series = df[['high', 'low']].copy()
            series['mean-variation'] = series['high']-series['low']

            series_highest = series.groupby(pd.Grouper(level=0, freq='MS')).mean()

            series_highest = series_highest.sort_values('mean-variation', ascending=False).head(1)

            i = series_highest.index.values[0]
            v = series_highest.values

            tup = (pd.to_datetime(i), v[0][2])

            tupla.append(tup)

        return tupla
