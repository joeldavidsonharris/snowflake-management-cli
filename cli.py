from schema import Schema, Or, Optional, And
from jinja2 import Template
import snowflake.connector
import argparse
import boto3
import yaml
import json


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
        if args.object_type == 'warehouses' or args.object_type == 'all':
            create_warehouses(snowflake['warehouses'])
        if args.object_type == 'databases' or args.object_type == 'all':
            create_databases(snowflake['envs'], snowflake['layers'], snowflake.get('settings', {}).get('database_prefix', ''))
        if args.object_type == 'frameworks' or args.object_type == 'all':
            create_frameworks(snowflake['frameworks'], snowflake.get('settings', {}).get('database_prefix', ''))
        if args.object_type == 'behaviour_packs' or args.object_type == 'all':
            create_behaviour_packs(snowflake['behaviour_packs'], snowflake['frameworks']['logging']['schema'], snowflake.get('settings', {}).get('database_prefix', ''))
        if args.object_type == 'integrations' or args.object_type == 'all':
            create_integrations(snowflake['integrations'])


def create_integrations(integrations):
    for storage_int in integrations.get('storage', []):
        name = storage_int['name']
        storage_provider = storage_int['storage_provider']
        storage_locations = storage_int['storage_locations']

        if storage_provider == 's3':
            s3_client = get_aws_connection(load_yaml_file('credentials.yml')['aws'], 's3')
            iam_client = get_aws_connection(load_yaml_file('credentials.yml')['aws'], 'iam')
            account = str(load_yaml_file('credentials.yml')['aws']['account'])

            # Create required buckets
            for bucket in storage_locations:
                try:
                    s3_client.create_bucket(Bucket=bucket)
                except s3_client.exceptions.BucketAlreadyOwnedByYou:
                   pass

            # Create role
            try:
                iam_client.create_role(
                    RoleName='role-snowflake-access',
                    AssumeRolePolicyDocument=json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [{
                            "Sid": "",
                            "Effect": "Allow",
                            "Principal": {
                                "AWS": account
                            },
                            "Action": "sts:AssumeRole",
                            "Condition": {
                                "StringEquals": {
                                    "sts:ExternalId": "0000"
                                }
                            }
                        }]
                    })
                )
            except iam_client.exceptions.EntityAlreadyExistsException:
                pass

            # Create IAM policy which grants access to buckets
            buckets = [f'arn:aws:s3:::{bucket}*' if bucket.endswith('/') else f'arn:aws:s3:::{bucket}/*' for bucket in storage_locations] if storage_locations[0] != '*' else '*'
            buckets_without_prefix = [bucket.split('/', 1)[0] for bucket in buckets] if buckets != '*' else buckets

            try:
                iam_client.create_policy(
                    PolicyName='policy-snowflake-access',
                    PolicyDocument=json.dumps({
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Action": [
                                  "s3:PutObject",
                                  "s3:GetObject",
                                  "s3:GetObjectVersion",
                                  "s3:DeleteObject",
                                  "s3:DeleteObjectVersion"
                                ],
                                "Resource": buckets
                            },
                            {
                                "Effect": "Allow",
                                "Action": [
                                    "s3:ListBucket",
                                    "s3:GetBucketLocation"
                                ],
                                "Resource": buckets_without_prefix
                            }
                        ]
                    })
                )
            except iam_client.exceptions.EntityAlreadyExistsException:
                pass

            # Grant policy-snowflake-access IAM policy to role-snowflake-access IAM role
            iam_client.attach_role_policy(
                RoleName='role-snowflake-access',
                PolicyArn=f'arn:aws:iam::{account}:policy/policy-snowflake-access'
            )

            # Create storage integration using prior details
            formatted_storage_locations = ','.join([f"'s3://{bucket}'" if bucket.endswith('/') else f"'s3://{bucket}/'" for bucket in storage_locations]) if storage_locations[0] != '*' else "'*'"
            role_arn = iam_client.get_role(RoleName='role-snowflake-access')['Role']['Arn']

            run_queries(f'''
                use role accountadmin;
                create storage integration if not exists {name}
                    type = external_stage
                    storage_provider = 's3'
                    enabled = true
                    storage_aws_role_arn = '{role_arn}'
                    storage_allowed_locations = ({formatted_storage_locations});
            ''', f'Create storage integration: {name}')

            # Get IAM user and external ID
            integration_attributes = run_queries(f'''
                use role accountadmin;
                desc integration {name};
            ''')[1].fetchall()

            snowflake_iam_user_arn = None
            snowflake_external_id = None

            for attribute in integration_attributes:
                if attribute[0] == 'STORAGE_AWS_IAM_USER_ARN':
                    snowflake_iam_user_arn = attribute[2]
                if attribute[0] == 'STORAGE_AWS_EXTERNAL_ID':
                    snowflake_external_id = attribute[2]

            # Update role trust policy using Snowflake IAM user and external ID
            iam_client.update_assume_role_policy(
                RoleName='role-snowflake-access',
                PolicyDocument=json.dumps({
                    "Version": "2012-10-17",
                    "Statement": [{
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": snowflake_iam_user_arn
                        },
                        "Action": "sts:AssumeRole",
                        "Condition": {
                            "StringEquals": {
                                "sts:ExternalId": snowflake_external_id
                            }
                        }
                    }]
                })
            )


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


def run_query(query_text, message=None):
    result = conn.cursor().execute(query_text.replace('\t', ''))
    if message is not None:
        conn.cursor().execute('use role sysadmin')
        conn.cursor().execute('insert into management.cli.action_log(action) values (%s)', (message))
    return result


def run_queries(query_text, message=None):
    result = conn.execute_string(query_text.replace('\t', ''))
    if message is not None:
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
    return get_snowflake_config_schema().validate(config)


def get_aws_connection(config, resource):
    return boto3.client(
        resource,
        aws_access_key_id=config['access_key'],
        aws_secret_access_key=config['secret_key']
    )


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
            Optional('warehouses'): [{
                'name': str,
                'warehouse_size': str,
                'auto_suspend': int
            }],
            Optional('envs'): [{
                'name': str,
                'data_retention_time_in_days': int
            }],
            Optional('layers'): [{
                'name': str,
                Optional('env'): bool
            }],
            Optional('behaviour_packs'): {
                Optional('disable_inactive_users'): {
                    'schema': str,
                    'inactive_days': int
                },
                Optional('log_account_usage'): {
                    'schema': str,
                    'reader': bool
                },
            },
            Optional('frameworks'): {
                'logging': {
                    'schema': str
                },
                Optional('security'): {
                    'schema': str,
                    'config_file': And(str, lambda x: x.endswith('.yml')),
                    Optional('role_prefix'): str
                }
            },
            Optional('integrations'): {
                'storage': [{
                    'name': str,
                    'storage_provider': 's3',
                    'storage_locations': [str]
                }]
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
