import os
import pyodbc
from munch import Munch
from dotenv import load_dotenv


load_dotenv('../.env')

SQL_PARAMS = Munch(
    {
        "SERVER": os.getenv("PRON_SERVER"),
        "SERVER_TYPE": os.getenv("PRON_SERVER_TYPE"),
        "DATABASE": os.getenv("LOAD_DATABASE"),
        "USERNAME": os.getenv("PRON_USERNAME"),
        "PASSWORD": os.getenv("PRON_PASSWORD"),
        "ODBC_DRIVER": os.getenv("PRON_ODBC_DRIVER"),
        "PORT": os.getenv("PRON_PORT"),
    }
)


def odbc_url_sqlalchemy():
    """Connect execution environment using ODBC interface."""
    connection_str = 'mssql+pyodbc://{}:{}@{}:{}/{}?driver={}'
    try:
        connection_url = connection_str.format(
            SQL_PARAMS.USERNAME,
            SQL_PARAMS.PASSWORD,
            SQL_PARAMS.SERVER,
            SQL_PARAMS.PORT,
            SQL_PARAMS.DATABASE,
            SQL_PARAMS.ODBC_DRIVER.replace(' ', '+')
        )

        return connection_url

    except ConnectionError as e:
        raise ConnectionError(f"""[Exception]
            Error trying to connect to {SQL_PARAMS.DATABASE} 
            (Azure SQL Server): """
                              ) from e
    except OSError as e:
        raise OSError("[ERROR] ") from e


def odbc_connection():
    """Connect execution environment using ODBC interface."""
    connection = "DRIVER={};SERVER={};PORT={};DATABASE={};UID={};PWD={}"
    try:
        sql_server_connection = pyodbc.connect(
            connection.format(
                SQL_PARAMS.ODBC_DRIVER,
                SQL_PARAMS.SERVER,
                SQL_PARAMS.PORT,
                SQL_PARAMS.DATABASE,
                SQL_PARAMS.USERNAME,
                SQL_PARAMS.PASSWORD,
            ),
            autocommit=True,
        )

        return sql_server_connection

    except ConnectionError as e:
        raise ConnectionError(f"""[Exception]
            Error trying to connect to {SQL_PARAMS.DATABASE} 
            (Azure SQL Server): """
                              ) from e
    except OSError as e:
        raise OSError("[ERROR] ") from e
