from typing import Dict

from ._column import create_column_statement


def create_table(schema_name: str, table_name: str, column_definitions: Dict) -> str:
    """Function for generating a create table statement

    :param str schema_name: The name of the schema that the table belongs to
    :param str table_name: The name of the table in question
    :param Dict column_definitions: The column definitions

    :return: The sql query
    :rtype: str
    """

    # create the columns
    column_statements = ',\n\t'.join(
        [
            create_column_statement(
                name=name,
                data_type=properties['data_type']
            )
            for name, properties in column_definitions.items()
        ]
    )

    # Create the table statement based on the column
    table_statement = f"CREATE TABLE {schema_name}.{table_name} (\n\t{column_statements}\n)"

    return table_statement
