from typing import List, Union


def create_column_statement(name: str,
                            data_type: str,
                            nullable: bool,
                            constraints: Union[List, None] = None,
                            **kwargs) -> str:
    """Function for generating column definitions

    :param str name: The name of the columns
    :param str data_type: The data type in question
    :param bool nullable: Whether the column is nullable
    :param Union[List, None] nullable: List of constraints
    :param kwargs:

    :return: The column sql definition
    :rtype: str
    """

    pk = False
    if constraints:
        for constraint in constraints:
            if constraint['type'] == 'p':
                pk = True
                nullable = False

    col_str = f"{name} {data_type}"
    if pk:
        col_str += " PRIMARY KEY"
        return col_str

    if not nullable:
        col_str += ' NOT NULL'

    return col_str


def alter_column_statement(name: str,
                           data_type: str,
                           nullable: bool,
                           constraints: Union[List, None] = None,
                           **kwargs) -> str:
    """Function for generating column definitions

    :param str name: The name of the columns
    :param str data_type: The data type in question
    :param bool nullable: Whether the column is nullable
    :param Union[List, None] nullable: List of constraints
    :param kwargs:

    :return: The column sql definition
    :rtype: str
    """

    pk = False
    if constraints:
        for constraint in constraints:
            if constraint['type'] == 'p':
                pk = True
                nullable = False

    col_str = f"ALTER COLUMN {name} TYPE {data_type}"
    col_str += ',\n'
    if not nullable:
        col_str += f"ALTER COLUMN {name} SET NOT NULL"
    else:
        col_str += f"ALTER COLUMN {name} DROP NOT NULL"

    return col_str
