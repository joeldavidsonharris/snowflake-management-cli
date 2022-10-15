# Snowflake Management CLI

The Snowflake Management CLI aims to simplify creating and managing a Snowflake account. Simply update YAML configuration files to match your desired state, before applying the changes via the CLI.

## Installation

Before beginning, you will need Git and Python 3.10 or later installed.

### Using Windows

```
git clone https://github.com/joeldavidsonharris/snowflake-management-cli.git
cd snowflake-management-cli
.\setup.bat
```

### Using Linux

```
git clone https://github.com/joeldavidsonharris/snowflake-management-cli.git
cd snowflake-management-cli
. setup.sh
```

### Adding Credentials

Create file `credentials.yml` with content:
```
snowflake:
  account: <Snowflake account name>
  user: <Snowflake user name>
  password: <Snowflake user password>
  role: accountadmin
  warehouse: admin_whs
```
> **Note:** The user must have the `accountadmin` role \
> **Note:** The warehouse `admin_whs` will be created automatically

## Usage

CLI behaviour is driven by the configuration YAML file `snowflake.yml`.

The `snowflake.yml` file will be automatically populated with an example Snowflake configuration. You can alter this configuration to match your needs before running the CLI. More documentation on the `snowflake.yml` schema coming soon!

### Perform inital setup

This will create Snowflake resources necessary for the CLI to function.
```
python cli.py initiate
```

### Apply configuration

This will apply `snowflake.yml` configuration.
```
# Apply all configurations
python cli.py apply all

# Apply account parameter configuration
python cli.py apply parameters

# Apply warehouse configuration
python cli.py apply warehouses

# Apply database configuration
python cli.py apply databases

# Apply framework configuration
python cli.py apply frameworks

# Apply behaviour pack configuration
python cli.py apply behaviour_packs

# Apply integration configuration
python cli.py apply integrations
```

### Auditing CLI actions

As a part of the CLI initiation process, a table will be created `management.cli.action_log`, this table will contain logs for all actions performed by the CLI.
```
use role sysadmin;
use warehouse admin_whs;

select *
from management.cli.action_log
order by action_complete_dtm desc;
```