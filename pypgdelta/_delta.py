from collections import OrderedDict
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

    return ';\n\n'.join(statement_list) + ';'
