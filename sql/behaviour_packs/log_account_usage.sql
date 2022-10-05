use role securityadmin;

grant imported privileges on database snowflake to role sysadmin;

use role sysadmin;
use warehouse admin_whs;

create schema if not exists {{ schema }};
create table if not exists {{ schema }}.query_history as
select * from snowflake.account_usage.query_history;
