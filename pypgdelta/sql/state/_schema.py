import psycopg2
import psycopg2.extras
from collections import OrderedDict
from typing import Dict, List


def get_schema_names(connection: psycopg2.extensions.connection) -> List[psycopg2.extras.RealDictRow]:
    """Function for getting the schema information from the given connection

    :param psycopg2.extensions.connection connection: The connection

    :return: List of rows using key-value pairs for the data
    :rtype: List[psycopg2.extras.RealDictRow]
    """
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        query = """SELECT *
                   FROM information_schema.schemata"""
        cursor.execute(query)

        results = cursor.fetchall()
    return results


def get_schemadict(connection: psycopg2.extensions.connection) -> Dict:
    """Function for getting the schema information from the given connection as a dict

    :param psycopg2.extensions.connection connection: The connection

    :return: Current database setup as a nested dictionary
    :rtype: Dict
    """
    schema_information = get_schema_names(connection)
    database_setup = OrderedDict(
        [
            (schema['schema_name'], OrderedDict())
            for schema in schema_information
        ]
    )

    return database_setup
