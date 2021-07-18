from collections import OrderedDict
from copy import deepcopy
from typing import Dict, List

from .sql import statements
from .sql.statements._constraint import create_constraint_statements


def get_delta(old_configuration: Dict, new_configuration: Dict) -> Dict:
    """Function to generate a delta based on the given configurations

    :param Dict old_configuration: The baseline configuration
    :param new_configuration: The desired configuration

    :return: Delta config
    :rtype: Dict
    """

    delta = OrderedDict()

    # Schema delta
    delta['schema'] = OrderedDict()
    delta['schema']['new'] = [schema for schema in new_configuration if schema not in old_configuration]

    # Table delta
    delta['tables'] = OrderedDict()
    delta['tables']['new'] = []
    delta['tables']['alter'] = []
    for schema_name, schema_config in new_configuration.items():
        schema_baseline = OrderedDict(
            [
                ('schema_name', schema_name)
            ]
        )
        tables = schema_config.get('tables', {})

        # Extract the table definitions
        for table_name, table_config in tables.items():
            table_baseline = deepcopy(schema_baseline)
            table_baseline['table_name'] = table_name
            column_definitions = table_config.get('columns', {})

            # Add the table definition if missing
            existing_definition = old_configuration.get(schema_name, {}).get('tables', {}).get(table_name)
            if not existing_definition:

                # Get the constraints for the new table
                constraints_delta = compare_constraints(
                    old_constraints={},
                    new_constraints=table_config.get('constraints')
                )

                # Add the constraints
                if constraints_delta:
                    table_baseline['constraints'] = constraints_delta

                table_baseline['column_definitions'] = column_definitions
                delta['tables']['new'].append(table_baseline)

            # Get the altered state if any
            else:
                alter = False
                existing_columns = existing_definition.get('columns', {})

                # Get any new column definitions
                new_columns = OrderedDict(
                    [
                        (k, v)
                        for k, v
                        in column_definitions.items()
                        if k not in existing_columns
                    ]
                )

                if new_columns:
                    table_baseline['new_column_definitions'] = new_columns
                    alter = True

                # Get updated columns
                alter_columns = OrderedDict(
                    [
                        (k, v)
                        for k, v
                        in column_definitions.items()
                        if k in existing_columns and (
                        not _compare_dict(
                            v,
                            existing_columns.get(k, {}),
                            [
                                'data_type',
                                'character_maximum_length',
                                'nullable',
                                'data_type_stmt'
                            ]
                        )
                    )
                    ]
                )

                if alter_columns:
                    table_baseline['alter_column_definitions'] = alter_columns
                    alter = True

                # Get the columns to delete
                delete_columns = OrderedDict(
                    [
                        (k, v)
                        for k, v
                        in existing_columns.items()
                        if k not in column_definitions
                    ]
                )
                if delete_columns:
                    table_baseline['delete_column_definitions'] = delete_columns
                    alter = True

                # Check constraints
                constraints_delta = compare_constraints(
                    old_constraints=existing_definition.get('constraints', {}),
                    new_constraints=table_config.get('constraints')
                )

                if constraints_delta:
                    table_baseline['constraints'] = constraints_delta
                    alter = True

                # Set the alter statements if needed
                if alter:
                    delta['tables']['alter'].append(table_baseline)
                # TODO: Add support for constraints

    return delta


def get_delta_statement(old_configuration: Dict, new_configuration: Dict) -> str:
    """Function to generate a delta based on the given configurations

    :param Dict old_configuration: The baseline configuration
    :param new_configuration: The desired configuration

    :return: Delta script
    :rtype: str
    """

    delta = get_delta(old_configuration, new_configuration)

    statement_list = []
    for schema in delta['schema']['new']:
        statement_list.append(statements.create_schema(schema))

    for table in delta['tables']['new']:
        statement_list.extend(
            statements.create_table(
                schema_name=table['schema_name'],
                table_name=table['table_name'],
                column_definitions=table['column_definitions']
            )
        )

        # Add constraints
        constraint_statements = create_constraint_statements(
            schema_name=table['schema_name'],
            table_name=table['table_name'],
            constraints=table['constraints'],
        )
        statement_list.extend(constraint_statements['create'])

    # Add the alter table statements
    for table in delta['tables']['alter']:
        constraint_statements = create_constraint_statements(
            schema_name=table['schema_name'],
            table_name=table['table_name'],
            constraints=table['constraints'],
        )

        # Drop the constraints
        statement_list.extend(constraint_statements['drop'])

        # Alter the columns
        statement_list.extend(
            statements.alter_table(
                schema_name=table['schema_name'],
                table_name=table['table_name'],
                new_column_definitions=table.get('new_column_definitions', {}),
                alter_column_definitions=table.get('alter_column_definitions', {}),
                delete_column_definitions=table.get('delete_column_definitions', {})
            )
        )

        # Add the create constraint statements
        statement_list.extend(constraint_statements['create'])

    if statement_list:
        return ';\n\n'.join(statement_list) + ';'

    return ''


def compare_constraints(old_constraints: Dict, new_constraints: Dict) -> Dict:
    """Function to figure out which constraints are new and or deleted

    :param Dict old_constraints: The existing configuration
    :param Dict new_constraints: The new configuration

    :return: The altered constraints
    :rtype: Dict
    """

    # Set the pks
    old_pk = old_constraints.get('primary_key', {})
    new_pk = new_constraints.get('primary_key', {})

    # Set the change object
    pk_change = OrderedDict()

    # Calculate the delta
    if old_pk and not new_pk:
        pk_change['drop_pk'] = old_pk
    elif new_pk and not old_pk:
        pk_change['new_pk'] = new_pk
    else:
        if new_pk['name'] != old_pk['name']:
            pk_change['drop_pk'] = old_pk
            pk_change['new_pk'] = new_pk
        elif new_pk['name'] != old_pk['name']:
            pk_change['drop_pk'] = old_pk
            pk_change['new_pk'] = new_pk

    return pk_change


def _compare_dict(old: Dict, new: Dict, keys: List[str]) -> bool:
    """Function for comparing a flat dictionary across specified keys

    :param Dict old: baseline dict
    :param Dict new: comparative dict
    :param List[str] keys: The keys to compare

    :return: Whether the dicts are equal for all keys
    :rtype: bool
    """

    try:
        for key in keys:
            if new[key] != old[key]:
                print(key)
                return False
    except:
        return False

    return True


def _check_constraints(active: List[Dict], desired: List[Dict]) -> bool:
    """Function for checking if the constraints match

    :param active:
    :param desired:
    :return: True if inconsistent, False if consistent
    """

    if not desired and len(active):
        return True
    elif len(active) != len(desired):
        return True
    elif not desired:
        return False

    active_sorted = sorted(active, key=lambda x: x['type'])
    desired_sorted = sorted(desired, key=lambda x: x['type'])

    for a, d in zip(active_sorted, desired_sorted):
        if not _compare_dict(a, d, ['type']):
            return True

    return False
