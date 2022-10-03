import snowflake.connector
from schema import Schema, Or, Optional, And
import argparse
import yaml

def main():
    args = parse_args()
    config = load_yaml_file('config.yml')
    get_config_schema().validate(config)
    if args.action == 'apply':
        pass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('action')
    return parser.parse_args()


def load_yaml_file(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)


def get_config_schema():
    return Schema({
        'snowflake': {
            Optional('database_prefix'): str,
            Optional('parameters'): [{str: Or(str, int, bool)}],
            'warehouses': [{
                str: {
                    'size': str,
                    'auto_suspend': int
                }
            }],
            'envs': [{
                str: {
                    'data_retention_days': int
                }
            }],
            'layers': [
                Or(str, {str: {'env': bool}})
            ],
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


if __name__ == '__main__':
    main()