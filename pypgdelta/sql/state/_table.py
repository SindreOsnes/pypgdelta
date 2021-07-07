import psycopg2
import psycopg2.extras
from collections import OrderedDict
from typing import Dict, List


def get_sql_tables_and_views(connection: psycopg2.extensions.connection) -> List[psycopg2.extras.RealDictRow]:
    """Function for getting the tables and views for a sql database

    :param psycopg2.extensions.connection connection: The connection

    :return: List of rows using key-value pairs for the data
    :rtype: List[psycopg2.extras.RealDictRow]
    """
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        query = """SELECT table_schema,
                          table_name,
                          table_type
                   FROM information_schema.tables"""
        cursor.execute(query)

        results = cursor.fetchall()
    return results


def get_table_dict(connection: psycopg2.extensions.connection) -> Dict:
    """Function for getting the tables and views for a sql database a dict

    :param psycopg2.extensions.connection connection: The connection

    :return: Current database setup as a nested dictionary
    :rtype: Dict
    """

    configuration = OrderedDict()
    table_information = get_sql_tables_and_views(connection)
    for table in table_information:
        if table['table_schema'] not in configuration:
            configuration[table['table_schema']] = OrderedDict(
                [
                    ('tables', OrderedDict()),
                    ('views', OrderedDict())
                ]
            )
        if table['table_type'] == 'BASE TABLE':
            configuration[table['table_schema']]['tables'][table['table_name']] = OrderedDict(
                [
                    ('columns', OrderedDict())
                ]
            )
        elif table['table_type'] == 'VIEW':
            configuration[table['table_schema']]['views'][table['table_name']] = OrderedDict(
                [
                    ('columns', OrderedDict())
                ]
            )

    return configuration
