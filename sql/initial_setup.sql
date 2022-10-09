use role accountadmin;

drop database if exists snowflake_sample_data;

grant execute task on account to role sysadmin;
grant execute task on account to role securityadmin;

use role sysadmin;

create warehouse if not exists admin_whs
warehouse_size = xsmall
auto_suspend = 60;

use role securityadmin;

grant usage on warehouse admin_whs to role securityadmin;

use role sysadmin;

create database if not exists management;
create schema if not exists management.cli;
create table if not exists management.cli.action_log (
    action varchar,
    status varchar default 'Success',
    action_complete_dtm timestamp default current_timestamp
);
