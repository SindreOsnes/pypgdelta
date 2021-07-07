from collections import OrderedDict
from copy import deepcopy
from typing import Iterable, Dict, Union

from ._column import create_column_config


def create_table_config(statements: Iterable[Dict], baseline: Union[Dict, None] = None) -> Dict:
    """Function for getting the table configuration based on supplied table statements

    :param List[Dict] statements: The statements upon which to base the configuration
    :param Dict baseline: A baseline configuration upon which to add the table configurations

    :return: The configuration
    :rtype: Dict
    """

    if not baseline:
        baseline = OrderedDict()

    baseline = deepcopy(baseline)

    # Identify create table statements
    table_statements = [statement for statement in statements if 'CreateStmt' in statement.get('stmt', {})]

    # Add each of the tables to the configuration
    for statement in table_statements:

        # Get the properties
        table_statement =statement['stmt']['CreateStmt']
        relation =table_statement['relation']
        schema_name = relation['schemaname']
        table_name = relation['relname']

        # Create schema if not in the baseline
        if schema_name not in baseline:
            baseline[schema_name] = OrderedDict(
                [
                    (
                        'tables',
                        OrderedDict()
                    )
                ]
            )

        # Add the table to the configuration
        # Schema
        schema = baseline[schema_name]

        # Table
        table_configuration = OrderedDict()
        schema['tables'][table_name] = table_configuration

        # Column
        column_configurations = OrderedDict()
        table_configuration['columns'] = column_configurations

        # Set the column configurations
        for element in table_statement.get('tableElts', []):
            if 'ColumnDef' in element:
                column_configuration = create_column_config(element['ColumnDef'])
                column_configurations.update(column_configuration)

    return baseline