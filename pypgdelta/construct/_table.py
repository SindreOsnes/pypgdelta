from collections import OrderedDict
from copy import deepcopy
from typing import Iterable, Dict, Union


def create_table_config(statements: Iterable[Dict], baseline: Union[Dict, None] = None) -> Dict:
    """Function for getting the baseline schema configuration based on create schema statements

    :param List[Dict] statements: The statements upon which to base the configuration
    :param Dict baseline: A baseline configuration upon which to add the table configurations

    :return: The configuration
    :rtype: Dict
    """

    if not baseline:
        baseline = OrderedDict()

    baseline = deepcopy(baseline)

    table_statements = [statement for statement in statements if 'CreateStmt' in statement.get('stmt', {})]

    return baseline