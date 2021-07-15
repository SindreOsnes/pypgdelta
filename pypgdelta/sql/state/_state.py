import psycopg2
from typing import Dict

from ._constraint import get_constraints_dict
from ._schema import get_schemadict
from ._table import get_table_dict


def get_state(connection: psycopg2.extensions.connection) -> Dict:
    """Function for getting the current database state as a dict

    :param psycopg2.extensions.connection connection: The connection

    :return: Current database setup as a nested dictionary
    :rtype: Dict
    """
    configuration = get_schemadict(connection)
    configuration.update(get_table_dict(connection))

    constraints = get_constraints_dict(connection)
    _recursive_update(configuration, constraints)

    return configuration


def _recursive_update(base: Dict, new: Dict):
    """Function for recursively updating dicts

    :param Dict base: base dict
    :param Dict new: new dict containing the updated values
    :return: Updates the base dict with data from the new one
    """

    for k, v in new.items():
        if k not in base or not isinstance(v, dict) or not isinstance(base[k], dict):
            base[k] = v
            continue

        _recursive_update(base[k], v)
