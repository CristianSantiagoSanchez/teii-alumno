""" Ejemplo de uso del paquete teii. """

import datetime
import logging
import matplotlib.pyplot as plt

import teii.finance as tf


def setup_logging(logging_level):
    """ Crea y configura logger.
    Parameters:
    -----------
    logging_level: Any (int)
        Defines the level of the logger

    Return:
    -------
    Logger
        Returns the logger created.
    """

    #   Tarea 3
    #   Configura logging para enviar la salida a un archivo

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename='example.log')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging_level)
    logger.info("Logger creado")

    return logger


def plot(pandas_series, ticker, logger):
    """ Dibuja una gráfica a partir de la serie de Pandas.
    Parameters:
    -----------
    pandas_series: pd.Series
        panda's series to draw
    ticker: List[str]
        Contains the list of the tickers
    logger: Logger
        Logger to print the aditional information
    """

    logger.info("Dibujando gráfica...")

    # for ser in pandas_series:
    for i, panda in enumerate(pandas_series):
        panda.plot(xlabel='Fecha', ylabel='Precio en USD', title=f"Evolución del Precio de {ticker[i]}")
        plt.show()  # ¡Necesario para que se muestre la gráfica en una ventana!


def main():
    """ Muestra como usar teii-finance. """

    logger = setup_logging(logging.INFO)

    logger.info("Inicio")

    # Define ticker y API key
    ticker = ['AMZN', 'IBM', 'TSLA']
    my_alpha_vantage_api_key = 'https://www.alphavantage.co/support/#api-key'

    # Crea cliente
    try:
        tf_client = tf.TimeSeriesFinanceClient(ticker,
                                               my_alpha_vantage_api_key,
                                               logging_level=logging.INFO)
    # Captura y muestra todas las excepciones
    except Exception as e:
        logger.error(f"{e}", exc_info=False)
    # Usa el cliente
    else:
        #   Tarea 3
        #   Filtra los datos para mostrar únicamente el año 2020

        # Genera una serie de Pandas con precio de cierre diario
        pd_series = tf_client.daily_volume(datetime.date(year=2020, month=1, day=1), datetime.date(year=2020, month=12, day=31))

        logger.info(pd_series)

        # Dibuja una gráfica a partir de la serie de Pandas
        plot(pd_series, ticker, logger)
    finally:
        logger.info("Fin")


if __name__ == "__main__":
    main()
