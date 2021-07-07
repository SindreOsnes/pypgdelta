def create_column_statement(name: str, data_type: str, **kwargs) -> str:
    """Function for generating column definitions

    :param str name: The name of the columns
    :param str data_type: The data type in question
    :param kwargs:

    :return: The column sql definition
    :rtype: str
    """

    col_str = f"{name} {data_type}"

    return col_str
