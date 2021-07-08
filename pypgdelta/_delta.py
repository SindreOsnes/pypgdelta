from collections import OrderedDict
from copy import deepcopy
from typing import Dict

from .sql import statements


def get_delta(old_configuration: Dict, new_configuration: Dict) -> Dict:
    """Function to generate a delta based on the given configurations

    :param Dict old_configuration: The baseline configuration
    :param new_configuration: The desired configuration

    :return: Delta config
    :rtype: Dict
    """

    delta = OrderedDict()

    # Schema delta
    delta['schema'] = OrderedDict()
    delta['schema']['new'] = [schema for schema in new_configuration if schema not in old_configuration]

    # Table delta
    delta['tables'] = OrderedDict()
    delta['tables']['new'] = []
    delta['tables']['alter'] = []
    for schema_name, schema_config in new_configuration.items():
        schema_baseline = OrderedDict(
            [
                ('schema_name', schema_name)
            ]
        )
        tables = schema_config.get('tables', {})

        # Extract the table definitions
        for table_name, table_config in tables.items():
            table_baseline = deepcopy(schema_baseline)
            table_baseline['table_name'] = table_name
            column_definitions = table_config.get('columns', {})

            # Add the table definition if missing
            existing_definition = old_configuration.get(schema_name, {}).get('tables', {}).get(table_name)
            if not existing_definition:
                table_baseline['column_definitions'] = column_definitions
                delta['tables']['new'].append(table_baseline)

            # Get the altered state if any
            else:
                alter = False
                existing_columns = existing_definition.get('columns', {})

                # Get any new column definitions
                new_columns = OrderedDict(
                    [
                        (k, v)
                        for k, v
                        in column_definitions.items()
                        if k not in existing_columns
                    ]
                )

                if new_columns:
                    table_baseline['new_columns'] = new_columns
                    alter = True

                # Set the alter statements if needed
                if alter:
                    delta['tables']['alter'].append(table_baseline)
                # TODO: Add support for alter statements

    return delta


def get_delta_statement(old_configuration: Dict, new_configuration: Dict) -> str:
    """Function to generate a delta based on the given configurations

    :param Dict old_configuration: The baseline configuration
    :param new_configuration: The desired configuration

    :return: Delta script
    :rtype: str
    """

    delta = get_delta(old_configuration, new_configuration)

    statement_list = []
    for schema in delta['schema']['new']:
        statement_list.append(statements.create_schema(schema))

    for table in delta['tables']['new']:
        statement_list.append(
            statements.create_table(
                schema_name=table['schema_name'],
                table_name=table['table_name'],
                column_definitions=table['column_definitions']
            )
        )

    return ';\n\n'.join(statement_list) + ';'
