from collections import OrderedDict
from typing import List, Dict


def construct_configuration(statements: List[Dict]) -> Dict:
    """Function for getting the configuration based on the input statements (as dicts)

    :param List[Dict] statements: The statements upon which to base the configuration

    :return: The configuration
    :rtype: Dict
    """

    statements_list = []
    for statement in statements:
        statements_list.extend(statement.get('stmts', []))

    return statements_list