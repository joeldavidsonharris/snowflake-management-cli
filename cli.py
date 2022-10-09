from schema import Schema, Or, Optional, And
from jinja2 import Template
import snowflake.connector
import argparse
import yaml


conn = None


def main(args):
    global conn
    conn = get_snowflake_connection(load_yaml_file('credentials.yml')['snowflake'])
    snowflake = read_snowflake_config(load_yaml_file('snowflake.yml'))['snowflake']

    if args.action == 'initiate':
        run_queries(load_file('sql/initial_setup.sql'), 'Perform initial setup')
        set_account_parameters(snowflake['parameters'])

    elif args.action == 'apply':
        if args.object_type == 'parameters' or args.object_type == 'all':
            set_account_parameters(snowflake['parameters'])
        if args.object_type == 'warehouse' or args.object_type == 'all':
            create_warehouses(snowflake['warehouses'])
        if args.object_type == 'database' or args.object_type == 'all':
            create_databases(snowflake['envs'], snowflake['layers'], snowflake.get('settings', {}).get('database_prefix', ''))
        if args.object_type == 'frameworks' or args.object_type == 'all':
            create_frameworks(snowflake['frameworks'], snowflake.get('settings', {}).get('database_prefix', ''))
        if args.object_type == 'behaviour_packs' or args.object_type == 'all':
            create_behaviour_packs(snowflake['behaviour_packs'], snowflake['frameworks']['logging']['schema'], snowflake.get('settings', {}).get('database_prefix', ''))


def create_frameworks(frameworks, database_prefix=''):
    if database_prefix != '':
        database_prefix += '_'

    for name, attributes in frameworks.items():
        if name == 'logging':
            schema = attributes['schema']
            database = schema.split('.')[0]
            rendered_sql = Template(load_file('sql/frameworks/logging.sql')).render(
                schema=f'{database_prefix}{schema}',
                database=f'{database_prefix}{database}'
            )
            run_queries(rendered_sql, f'Create framework: {name}')
        if name == 'security':
            schema = attributes['schema']
            database = schema.split('.')[0]
            config_file = attributes['config_file']
            role_prefix = attributes['role_prefix']
            rendered_sql = Template(load_file('sql/frameworks/security.sql')).render(
                schema=f'{database_prefix}{schema}',
                database=f'{database_prefix}{database}'
            )
            run_queries(rendered_sql, f'Create framework: {name}')


def create_behaviour_packs(behaviour_packs, logging_schema, database_prefix=''):
    if database_prefix != '':
        database_prefix += '_'

    for name, attributes in behaviour_packs.items():
        if name == 'disable_inactive_users':
            schema = attributes['schema']
            database = schema.split('.')[0]
            inactive_days = attributes['inactive_days']
            rendered_sql = Template(load_file('sql/behaviour_packs/disable_inactive_users.sql')).render(
                schema=f'{database_prefix}{schema}',
                database=f'{database_prefix}{database}',
                logging_schema=f'{database_prefix}{logging_schema}',
                inactive_days=inactive_days
            )
            run_queries(rendered_sql, f'Create behaviour pack: {name}')
        if name == 'log_account_usage':
            schema = attributes['schema']
            database = schema.split('.')[0]
            reader = str(attributes['reader']).lower()
            rendered_sql = Template(load_file('sql/behaviour_packs/log_account_usage.sql')).render(
                schema=f'{database_prefix}{schema}',
                database=f'{database_prefix}{database}',
                logging_schema=f'{database_prefix}{logging_schema}',
                reader=reader
            )
            run_queries(rendered_sql, f'Create behaviour pack: {name}')


def create_databases(environments, data_layers, database_prefix=''):
    if database_prefix != '':
        database_prefix += '_'

    for env in environments:
        env_name = env['name']
        attributes = '\n'.join([f'{key} = {value}' for key, value in env.items() if key != 'name'])
        for layer in [layer for layer in data_layers if layer.get('env', True)]:
            layer_name = layer['name']
            name = f'{database_prefix}{env_name}_{layer_name}'
            run_queries(f'''
                use role sysadmin;
                create database if not exists {name}
                {attributes};
            ''', f'Create database: {name}')
    
    for layer in [layer for layer in data_layers if not layer.get('env', True)]:
        layer_name = layer['name']
        name = f'{database_prefix}{layer_name}'
        run_queries(f'''
            use role sysadmin;
            create database if not exists {name}
            {attributes};
        ''', f'Create database: {name}')


def create_warehouses(warehouses):
    for warehouse in warehouses:
        name = warehouse['name']
        attributes = '\n'.join([f'{key} = {value}' for key, value in warehouse.items() if key != 'name'])
        run_queries(f'''
            use role sysadmin;
            create warehouse if not exists {name}
            {attributes};
        ''', f'Create warehouse: {name}')


def set_account_parameters(parameters):
    account_parameters = '\n'.join([f'{key} = {value}' for key, value in parameters.items()])
    run_queries(f'''
        use role accountadmin;
        alter account set
        {account_parameters};
    ''', 'Set account parameters')


def run_query(query_text, message):
    result = conn.cursor().execute(query_text.replace('\t', ''))
    conn.cursor().execute('use role sysadmin')
    conn.cursor().execute('insert into management.cli.action_log(action) values (%s)', (message))
    return result


def run_queries(query_text, message):
    result = conn.execute_string(query_text.replace('\t', ''))
    conn.cursor().execute('use role sysadmin')
    conn.cursor().execute('insert into management.cli.action_log(action) values (%s)', (message))
    return result


def load_yaml_file(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)


def load_file(path):
    with open(path, 'r') as file:
        return file.read()


def read_snowflake_config(config):
    get_snowflake_config_schema().validate(config)
    return config


def get_snowflake_connection(config):
    return snowflake.connector.connect(
        account=config['account'],
        user=config['user'],
        password=config['password'],
        role=config['role'],
        warehouse=config['warehouse']
    )


def get_snowflake_config_schema():
    return Schema({
        'snowflake': {
            Optional('settings'): {Optional('database_prefix'): str},
            Optional('parameters'): {str: Or(str, int, bool)},
            'warehouses': [{
                'name': str,
                'warehouse_size': str,
                'auto_suspend': int
            }],
            'envs': [{
                'name': str,
                'data_retention_time_in_days': int
            }],
            'layers': [{
                'name': str,
                Optional('env'): bool
            }],
            'behaviour_packs': {
                Optional('disable_inactive_users'): {
                    'schema': str,
                    'inactive_days': int
                },
                Optional('log_account_usage'): {
                    'schema': str,
                    'reader': bool
                },
            },
            'frameworks': {
                'logging': {
                    'schema': str
                },
                Optional('security'): {
                    'schema': str,
                    'config_file': And(str, lambda x: x.endswith('.yml')),
                    Optional('role_prefix'): str
                }
            }
        }
    })


def parse_args():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action')
    initiate_parser = subparsers.add_parser('initiate')
    apply_parser = subparsers.add_parser('apply')
    apply_parser.add_argument('object_type')
    return parser.parse_args()


if __name__ == '__main__':
    main(parse_args())
