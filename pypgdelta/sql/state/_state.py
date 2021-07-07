import psycopg2
from typing import Dict

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

    return configuration
