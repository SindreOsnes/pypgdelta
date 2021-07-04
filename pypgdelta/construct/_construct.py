from collections import OrderedDict
from typing import List, Dict


def construct_configuration(statements: List[Dict]) -> Dict:
    """Function for getting the configuration based on the input statements (as dicts)

    :param List[Dict] statements: The statements upon which to base the configuration

    :return: The configuration
    :rtype: Dict
    """

    statements_list = []
    for statement in statements:
        statements_list.extend(statement.get('stmts', []))

    schema_statements = [statement for statement in statements_list if 'CreateSchemaStmt' in statement.get('stmt', {})]
    configuration = OrderedDict(
        [
            (statement['stmt']['CreateSchemaStmt']['schemaname'], OrderedDict())
            for statement in schema_statements
        ]
    )

    return configuration
