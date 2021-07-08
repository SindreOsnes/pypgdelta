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

    # Get the column udt type
    col_type = tuple([name['String']['str'] for name in column_def['typeName']['names']])

    # Set the appropriate type
    if col_type == ('pg_catalog', 'int8'):
        column_config['data_type'] = 'bigint'

    # Check nullability
    column_config['nullable'] = True
    for constraint in column_def.get('constraints', []):
        if constraint.get('Constraint', {}).get('contype', None) == "CONSTR_NOTNULL":
            column_config['nullable'] = True

    if 'data_type' not in column_config:
        raise TypeError(f'Unable to handle column type {col_type}')
    return configuration
