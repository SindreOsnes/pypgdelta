import psycopg2
from typing import Dict

from .schema import get_schemadict


def get_state(connection: psycopg2.extensions.connection) -> Dict:
    """Function for getting the current database state as a dict

    :param psycopg2.extensions.connection connection: The connection

    :return: Current database setup as a nested dictionary
    :rtype: Dict
    """
    baseline = get_schemadict(connection)

    return baseline
