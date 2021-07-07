from collections import OrderedDict
from typing import Dict


def create_column_config(column_def: Dict) -> Dict:
    """Function for getting the column object based on the supplied definition

    :param List[Dict] column_def: The column definition upon which to base the output structure

    :return: The configuration
    :rtype: Dict
    """

    configuration = OrderedDict()
    column_config = OrderedDict()
    configuration[column_def['colname']] = column_config

    return configuration
