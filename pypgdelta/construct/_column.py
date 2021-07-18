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
        column_config['data_type_stmt'] = 'bigint'
        column_config['character_maximum_length'] = None

    # Deal with varchar
    elif col_type == ('pg_catalog', 'varchar'):
        column_config['data_type'] = 'character varying'
        column_config['data_type_stmt'] = f'varchar'

        # Determine the maximum character length
        max_length = None
        for type_mod in column_def['typeName'].get('typmods', []):
            if 'A_Const' in type_mod:
                max_length = type_mod['A_Const'].get('val', {}).get('Integer', {}).get('ival', None)
                break

        if max_length is not None:
            column_config['data_type_stmt'] = f'varchar({max_length})'

        column_config['character_maximum_length'] = max_length

    # Deal with uuid columns
    if col_type == ('uuid', ):
        column_config['data_type'] = 'uuid'
        column_config['data_type_stmt'] = 'uuid'
        column_config['character_maximum_length'] = None

    # Check nullability
    column_config['nullable'] = True
    for constraint in column_def.get('constraints', []):
        if constraint.get('Constraint', {}).get('contype', None) == "CONSTR_NOTNULL":
            column_config['nullable'] = False

    # Check primary key status
    column_config['constraints'] = []
    for constraint in column_def.get('constraints', []):
        if constraint.get('Constraint', {}).get('contype', None) == "CONSTR_PRIMARY":
            column_config['nullable'] = False
            column_config['constraints'].append(
                {
                    'type': 'p'
                }
            )

    if 'data_type' not in column_config:
        raise TypeError(f'Unable to handle column type {col_type}')
    return configuration
