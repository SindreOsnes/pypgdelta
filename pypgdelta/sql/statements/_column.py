def create_column_statement(name: str, data_type: str, nullable: bool, **kwargs) -> str:
    """Function for generating column definitions

    :param str name: The name of the columns
    :param str data_type: The data type in question
    :param bool nullable: Whether the column is nullable
    :param kwargs:

    :return: The column sql definition
    :rtype: str
    """

    col_str = f"{name} {data_type}"
    if not nullable:
        col_str += ' NOT NULL'

    return col_str


def alter_column_statement(name: str, data_type: str, nullable: bool, **kwargs) -> str:
    """Function for generating column definitions

    :param str name: The name of the columns
    :param str data_type: The data type in question
    :param bool nullable: Whether the column is nullable
    :param kwargs:

    :return: The column sql definition
    :rtype: str
    """

    col_str = f"ALTER COLUMN {name} TYPE {data_type}"
    if not nullable:
        col_str += ',\n'
        col_str += f"ALTER COLUMN {name} SET NOT NULL"

    return col_str
