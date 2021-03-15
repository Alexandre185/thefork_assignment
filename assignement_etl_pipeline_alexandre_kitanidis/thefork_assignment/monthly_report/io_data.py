import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import csv
import decorators


@decorators.timed
def load_csv_to_df(filepath):
    """
    Loads a csv file content as a pandas dataframe

    :param filepath: path of the csv file
    :return: dataframe with the csv content
    """
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print('csv file not found')
    except pd.errors.ParserError:
        print('file has incorrect format')
    else:
        return df


@decorators.timed
def save_df_as_csv(df, filepath):
    """
    Creates a csv file on the specified path and saves it the content of a pandas dataframe

    :param df: dataframe which content will be saved on the csv file
    :param filepath: path to where to create the csv file

    """
    df.to_csv(filepath, index=False)


def psql_insert_copy(table, conn, keys, data_iter):
    """
    Execute SQL statement inserting data

    :param table: pandas.io.sql.SQLTable
    :param conn: sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
    :param keys: list of str
    :param data_iter: Iterable that iterates the values to be inserted
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        table_name = '{}.{}'.format(table.schema, table.name) if table.schema else table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)


@decorators.timed
def send_df_to_postgres_db(df, username, password, database,
                           host='localhost', port=5432, table='monthly_restaurants_report'):
    """
    Sends the dataframe content to a postgresSQL table in the specified database. If the table doesn't exist,
    it is created, otherwise the content of the dataframe is append to the existing table.

    :param df: the dataframe which content is sent to the postgres table
    :param username: username of the postgres instance
    :param password: password of the postgres instance
    :param database: name of the database we want to send the data to
    :param host: host of the postgres instance
    :param port: port on which connect to the postgres instance
    :param table: name of the table the data is sent to
    """

    try:
        connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database)
        engine = create_engine(connection_string)
        df.to_sql(table, engine, if_exists='append', method=psql_insert_copy)
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        print(error)
