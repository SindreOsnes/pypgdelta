def create_schema(name: str) -> str:
    """Function for generating create schema statements

    :param str name: The name of the schema

    :return: Create statement
    :rtype: str
    """

    statement = f"CREATE SCHEMA {name}"

    return statement
