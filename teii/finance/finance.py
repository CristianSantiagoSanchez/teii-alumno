""" Finance Client classes """


import json
import logging
import os
import pandas as pd
import requests
import typing

from abc import ABC, abstractclassmethod, abstractmethod
from pathlib import Path
from typing import Optional, Union

from teii.finance import FinanceClientInvalidAPIKey
from teii.finance import FinanceClientAPIError
from teii.finance import FinanceClientInvalidData
from teii.finance import FinanceClientIOError


class FinanceClient(ABC):
    """ Wrapper around the Finance API.

    ...

    Attributes:
    ----------
    _ticker: typing.List[str]
        list of ticker
    api_key: str, optional
        key to search the URLs (default is None)
    _logger: Logger
        logger of the class
    _json_data: typing.List[dict]
        contains the json data readed
    _json_metadata: typing.List[dict]
        contains the json metadata readed
    _data_frame: typing.List[pd.DataFrame]
        contains the list of json's data frames
    _FinanceBaseQueryURL: str
        Base query URL

    Methods:
    --------
    _setup_logging(logging_level,logging_file)
        Defines the logger of the class
    _build_base_query_url(cls)
        Return base query URL
    _build_base_query_url_params(ticker)
        Return base query URL parameters.
    _query_api(ticker)
        Query API endpoint
    _build_query_metadata_key()
        Return metadata query key
    _build_query_data_key()
        Return data query key
    _process_query_response(response)
        Preprocess query data
    _process_query_response(response)
        Preprocess query data
    _validate_query_data(ticker)
        Validate query data
    to_pandas()
        Return pandas data frame from json data
    to_csv(path2file)
        Write the first json data into csv file 'path2file'.
    """

    _FinanceBaseQueryURL = "https://www.alphavantage.co/query?"  # Class variable

    def __init__(self, ticker: typing.List[str],
                 api_key: Optional[str] = None,
                 logging_level: Union[int, str] = logging.WARNING,
                 logging_file: Optional[str] = None) -> None:
        """ FinanceClient constructor.
        Parameters:
        ----------
        ticker: typing.List[str]
            Contains the list of tickers
        api_key: str, optional
            key to search the URLs (default is None)
        logging_level: Union[int, str]
            defines the level of the logger (default is logging.WARNING)
        logging_file: str, optional
            file to print the log (default is None)

        Raises:
        -------
        FinanceClientInvalidAPIKey
            The API key is wrong type or not exist
        """

        self._ticker = ticker
        self._api_key = api_key
        self._json_data: typing.List[dict] = []
        self._json_metadata: typing.List[dict] = []

        # Logging configuration
        self._setup_logging(logging_level, logging_file)

        # Finance API key configuration
        self._logger.info("API key configuration")
        if not self._api_key:
            self._api_key = os.getenv("TEII_FINANCE_API_KEY")
        if not self._api_key or not isinstance(self._api_key, str):
            raise FinanceClientInvalidAPIKey(f"{self.__class__.__qualname__} operation failed")

        for t in self._ticker:
            # Query Finance API obtiene response - necesita ticker
            self._logger.info("Finance API access...")
            response = self._query_api(t)

            # Process query response - añade data y metadata a las listas - necesita response
            self._logger.info("Finance API query response processing...")
            self._process_query_response(response)

            # Validate query data compara algo de su metada con el ticker - necesita ticker
            self._logger.info("Finance API query data validation...")
            self._validate_query_data(t)

        # Panda's Data Frame
        self._data_frame: typing.List[pd.DataFrame] = []

    def _setup_logging(self,
                       logging_level: Union[int, str],
                       logging_file: Optional[str]) -> None:
        """ Defines the logger of the class
        Parameters:
        ----------
        logging_level: Union[int, str]
            defines the level of the logger
        logging_file: str, optional
            file to print the log
        """
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging_level)

    @classmethod
    def _build_base_query_url(cls) -> str:
        """Return base query URL.

        URL is independent from the query type.
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?PARAMS

        Returns:
        -------
        str
            Return the string that constains the BaseQueryURL
        """

        return cls._FinanceBaseQueryURL

    @abstractmethod
    def _build_base_query_url_params(self, ticker: str) -> str:
        """ Return base query URL parameters.

        Parameters are dependent on the query type:
            https://www.alphavantage.co/documentation/
        URL format:
            https://www.alphavantage.co/query?PARAMS

        Parameters:
        ----------
        ticker: str
            Contain the ticker
        """

        pass

    def _query_api(self, t: str) -> requests.Response:
        """ Query API endpoint.
        Parameters:
        ----------
        t: str
            Contain the ticker

        Returns:
        -------
        Response
            Return the response of the request.

        Raises:
        ------
        FinanceClientAPIError
            Unsuccessful API access
        """

        try:
            response = requests.get(f"{self.__class__._build_base_query_url()}{self._build_base_query_url_params(t)}")
            assert response.status_code == 200
        except Exception as e:
            raise FinanceClientAPIError(f"Unsuccessful API access "
                                        f"[URL: {response.url}, status: {response.status_code}]") from e
        else:
            self._logger.info(f"Successful API access "
                              f"[URL: {response.url}, status: {response.status_code}]")
        return response

    @classmethod
    def _build_query_metadata_key(self) -> str:
        """ Return metadata query key.
        Returns:
        -------
        str
            Return 'Meta Data'
        """

        return "Meta Data"

    @abstractclassmethod
    def _build_query_data_key(cls) -> str:
        """ Return data query key. """

        pass

    def _process_query_response(self, response: requests.Response) -> None:
        """ Preprocess query data.
        Parameters:
        ----------
        response: Response
            Contain the response of the request
        Raises:
        ------
        FinanceClientInvalidData
            The data is invalid
        """

        try:
            json_data_downloaded = response.json()
            json_metadata = json_data_downloaded[self._build_query_metadata_key()]
            json_data = json_data_downloaded[self._build_query_data_key()]
            self._json_metadata.append(json_metadata)
            self._json_data.append(json_data)
        except Exception as e:
            raise FinanceClientInvalidData("Invalid data") from e
        else:
            self._logger.info("Metadata and data fields found")

        self._logger.info(f"Metadata: '{json_metadata}'")
        self._logger.info(f"Data: '{json.dumps(json_data)[0:218]}...'")

    @abstractmethod
    def _validate_query_data(self, ticker: str) -> None:
        """ Validate query data.
        Parameters:
        ----------
        ticker: str
            Contain the ticker
        """

        pass

    def to_pandas(self) -> pd.DataFrame:
        """ Return pandas data frame from json data.
        Returns:
        -------
        List[pd.DataFrame]
            Return the list of data frames of the tickers
        """

        assert self._data_frame is not None

        return self._data_frame

    def to_csv(self, path2file: Path) -> Path:
        """ Write json data into csv file 'path2file'.
        Parameters:
        -----------
        psth2file: Path
            Constains the path to the file
        Returns:
        -------
        Path
            Return the path to the file
        Raises:
        ------
        FinanceClientIOError
            Unable to write json data into file
        """

        assert self._data_frame is not None

        try:
            self._data_frame[0].to_csv(path2file)
        except (IOError, PermissionError) as e:
            raise FinanceClientIOError(f"Unable to write json data into file '{path2file}'") from e

        return path2file
