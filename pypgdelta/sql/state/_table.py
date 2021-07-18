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
        query = """SELECT t.table_schema,
                          t.table_name,
                          t.table_type,
                          c.character_maximum_length,
                          c.column_name,
                          c.data_type,
                          c.is_nullable
                   FROM information_schema.columns c
                   INNER JOIN information_schema.tables t
                   ON t.table_schema = c.table_schema AND t.table_name = c.table_name"""
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
    for table_col in table_information:

        # Instantiate the schema object
        if table_col['table_schema'] not in configuration:
            configuration[table_col['table_schema']] = OrderedDict(
                [
                    ('tables', OrderedDict()),
                    ('views', OrderedDict())
                ]
            )

        # Limit operations to selected table/view definition
        schema_definition = configuration[table_col['table_schema']]
        if table_col['table_type'] == 'BASE TABLE':
            if table_col['table_name'] not in schema_definition['tables']:
                schema_definition['tables'][table_col['table_name']] = OrderedDict(
                    [
                        ('columns', OrderedDict())
                    ]
                )
            table_definition = schema_definition['tables'][table_col['table_name']]
        else:
            if table_col['table_name'] not in schema_definition['tables']:
                schema_definition['views'][table_col['table_name']] = OrderedDict(
                    [
                        ('columns', OrderedDict())
                    ]
                )
            table_definition = schema_definition['views'][table_col['table_name']]

        table_definition['columns'].update(_generate_column_definitions(table_col))

    return configuration


def _generate_column_definitions(column_definition: psycopg2.extras.RealDictRow) -> Dict:
    """Function for generating the column definition object

    :param psycopg2.extras.RealDictRow column_definition: The column definition from the database

    :return: The column setup as a dict
    :rtype: Dict
    """

    column_setup = OrderedDict()
    column_information = OrderedDict()
    column_setup[column_definition['column_name']] = column_information
    column_information['data_type'] = column_definition['data_type']
    column_information['character_maximum_length'] = column_definition['character_maximum_length']
    column_information['nullable'] = column_definition['is_nullable'] == 'YES'

    # Set the data type statement
    if column_information['data_type'] == 'bigint':
        column_information['data_type_stmt'] = "bigint"

    elif column_information['data_type'] == 'character varying':
        if column_information['character_maximum_length'] is not None:
            column_information['data_type_stmt'] = f"varchar({column_information['character_maximum_length']})"
        else:
            column_information['data_type_stmt'] = f"varchar"
    elif column_information['data_type'] == 'uuid':
        column_information['data_type_stmt'] = "uuid"

    else:
        column_information['data_type_stmt'] = None

    return column_setup
