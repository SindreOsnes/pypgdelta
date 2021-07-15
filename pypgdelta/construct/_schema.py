from collections import OrderedDict
from typing import Iterable, Dict


def create_schema_baseline(statements: Iterable[Dict]) -> Dict:
    """Function for getting the baseline schema configuration based on create schema statements

    :param List[Dict] statements: The statements upon which to base the configuration

    :return: The configuration
    :rtype: Dict
    """

    schema_statements = [statement for statement in statements if 'CreateSchemaStmt' in statement.get('stmt', {})]
    configuration = OrderedDict(
        [
            (
                statement['stmt']['CreateSchemaStmt']['schemaname'], OrderedDict(
                    [
                        (
                            'tables',
                            OrderedDict()
                        )
                    ]
                )
            )
            for statement in schema_statements
        ]
    )

    return configuration
