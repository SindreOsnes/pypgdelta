from collections import OrderedDict
from typing import Dict


def create_constraint_statements(schema_name: str,
                                 table_name: str,
                                 constraints: Dict,
                                 **kwargs) -> Dict:
    """Function for generating primary key definitions definitions

    :param str schema_name: The name of the schema the table belongs to
    :param str table_name: The name of the table
    :param Dict constraints: The constraint definitions
    :param kwargs:

    :return: The constraint statements
    :rtype: str
    """

    statements = OrderedDict(
        [
            ('drop', []),
            ('create', [])
        ]
    )

    if 'new_pk' in constraints:
        statement = f"ALTER TABLE {schema_name}.{table_name} ADD CONSTRAINT {constraints['new_pk']['name']}"
        statement += f" PRIMARY KEY({','.join(constraints['new_pk']['columns'])})"
        statements['create'].append(statement)
    if 'drop_pk' in constraints:
        statement = f"ALTER TABLE {schema_name}.{table_name} DROP CONSTRAINT IF EXISTS {constraints['drop_pk']['name']} CASCADE"
        statements['drop'].append(statement)

    return statements
