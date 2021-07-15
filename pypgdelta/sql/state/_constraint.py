from collections import OrderedDict
from typing import List, Dict

import psycopg2


def get_constraints_sql(connection: psycopg2.extensions.connection) -> List[psycopg2.extras.RealDictRow]:
    """Function for getting the constraints for a sql database

    :param psycopg2.extensions.connection connection: The connection

    :return: List of rows using key-value pairs for the data
    :rtype: List[psycopg2.extras.RealDictRow]
    """
    with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
        query = """SELECT con.conname      AS name,
                          con.contype      AS type,
                          ccu.table_schema AS schema,
                          ccu.table_name   AS table,
                          ccu.column_name  AS column
                   FROM pg_catalog.pg_constraint con
                            INNER JOIN pg_catalog.pg_class rel
                                       ON rel.oid = con.conrelid
                            INNER JOIN pg_catalog.pg_namespace nsp
                                       ON nsp.oid = con.connamespace
                            LEFT JOIN information_schema.constraint_column_usage ccu
                                      ON con.conname = ccu.constraint_name
                                          AND nsp.nspname = ccu.constraint_schema"""
        cursor.execute(query)

        results = cursor.fetchall()
    return results


def get_constraints_dict(connection: psycopg2.extensions.connection) -> Dict:
    """Function for getting the constraints for a sql database as a dict

    :param psycopg2.extensions.connection connection: The connection

    :return: Current database setup as a nested dictionary
    :rtype: Dict
    """

    configuration = OrderedDict()
    constraint_information = get_constraints_sql(connection)

    for constraint in constraint_information:

        # Set the schema
        if constraint['schema'] not in configuration:
            configuration[constraint['schema']] = OrderedDict(
                [
                    ('tables', OrderedDict())
                ]
            )

        schema_information = configuration[constraint['schema']]

        # Set the table
        if constraint['table'] not in schema_information['tables']:
            schema_information['tables'][constraint['table']] = OrderedDict(
                [
                    ('columns', OrderedDict())
                ]
            )

        table_information = schema_information['tables'][constraint['table']]

        # Set the column
        if constraint['column'] not in table_information['columns']:
            table_information['columns'][constraint['column']] = OrderedDict(
                [
                    ('constraints', [])
                ]
            )

        column_information = table_information['columns'][constraint['column']]

        column_information['constraints'].append(
            {
                'name': constraint['name'],
                'type': constraint['type']
            }
        )

    return configuration
