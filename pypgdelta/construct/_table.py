from collections import OrderedDict
from copy import deepcopy
from typing import Iterable, Dict, Union


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
            baseline[schema_name] = OrderedDict()

        # Add the table to the configuration
        table_configuration = OrderedDict
        schema = baseline[schema_name]
        schema[table_name] = table_configuration

    return baseline