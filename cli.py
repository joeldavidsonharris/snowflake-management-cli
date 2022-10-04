from schema import Schema, Or, Optional, And
import snowflake.connector
import argparse
import yaml

def main(args):
    snowflake = read_snowflake_config(load_yaml_file('snowflake.yml'))
    if args.action == 'apply':
        if args.object_type == 'warehouse':
            create_warehouses(snowflake['warehouses'])


def create_warehouses(warehouses):
    for warehouse in warehouses:
        pass # TODO: Add Snowflake connector logic here


def load_yaml_file(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)


def read_snowflake_config(config):
    get_snowflake_config_schema().validate(config)
    return config


def get_snowflake_config_schema():
    return Schema({
        'snowflake': {
            Optional('settings'): {Optional('database_prefix'): str},
            Optional('parameters'): {str: Or(str, int, bool)},
            'warehouses': [{
                'name': str,
                'size': str,
                'auto_suspend': int
            }],
            'envs': [{
                'name': str,
                'data_retention_days': int
            }],
            'layers': [{
                'name': str,
                Optional('env'): bool
            }],
            'behaviour_packs': {
                Optional('disable_inactive_users'): {
                    'inactive_days': int
                },
                Optional('log_account_usage'): {
                    'schema': str
                },
            },
            'frameworks': {
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
    parser.add_argument('action')
    parser.add_argument('object_type')
    return parser.parse_args()


if __name__ == '__main__':
    main(parse_args())