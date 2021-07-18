from typing import Dict, List

from ._column import alter_column_statement, create_column_statement


def create_table(schema_name: str, table_name: str, column_definitions: Dict) -> List[str]:
    """Function for generating a create table statement

    :param str schema_name: The name of the schema that the table belongs to
    :param str table_name: The name of the table in question
    :param Dict column_definitions: The column definitions

    :return: The sql query
    :rtype: List[str]
    """

    # create the columns
    column_statements = ',\n\t'.join(
        [
            create_column_statement(
                name=name,
                data_type=properties['data_type_stmt'],
                nullable=properties['nullable']
            )
            for name, properties in column_definitions.items()
        ]
    )

    # Create the table statement based on the column
    table_statement = f"CREATE TABLE {schema_name}.{table_name} (\n\t{column_statements}\n)"

    return [table_statement]


def alter_table(schema_name: str, table_name: str,
                new_column_definitions: Dict,
                alter_column_definitions: Dict,
                delete_column_definitions: Dict) -> List[str]:
    """Function for generating an alter table statement

    :param str schema_name: The name of the schema that the table belongs to
    :param str table_name: The name of the table in question
    :param Dict new_column_definitions: The column definitions to be added
    :param Dict alter_column_definitions: The column definitions to be altered
    :param Dict delete_column_definitions: The column definitions to be deleted

    :return: The sql query
    :rtype: List[str]
    """

    # create the columns
    new_column_statements = ',\n'.join(
        [
            'ADD COLUMN ' + create_column_statement(
                name=name,
                data_type=properties['data_type_stmt'],
                nullable=properties['nullable'],
                constraints=properties.get('constraints', [])
            )
            for name, properties in new_column_definitions.items()
        ]
    )

    alter_column_statements = ',\n'.join(
        [
            alter_column_statement(
                name=name,
                data_type=properties['data_type_stmt'],
                nullable=properties['nullable'],
                constraints=properties.get('constraints', [])
            )
            for name, properties in alter_column_definitions.items()
        ]
    )

    delete_column_statements = ',\n'.join(
        [
            f"DROP COLUMN {name}"
            for name, properties in delete_column_definitions.items()
        ]
    )

    table_statement = ''

    if new_column_definitions:
        # Create the alter table statement based on the columns
        table_statement = f"ALTER TABLE {schema_name}.{table_name} \n{new_column_statements}"

    # Create the alter column statements
    if alter_column_definitions:

        # Set the column alterations to be a separate command
        if table_statement:
            table_statement += ';\n\n'

        table_statement += f"ALTER TABLE {schema_name}.{table_name} \n{alter_column_statements}"

    # Create the delete column statements
    if delete_column_definitions:

        # Set the column deletions to be a separate command
        if table_statement:
            table_statement += ';\n\n'

        table_statement += f"ALTER TABLE {schema_name}.{table_name} \n{delete_column_statements}"

    # Return the statement as a list
    if table_statement:
        return [table_statement]
    return []
