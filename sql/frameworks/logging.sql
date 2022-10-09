use role sysadmin;
use warehouse admin_whs;

create schema if not exists {{ schema }};
create table if not exists {{ schema }}.procedure_logs (
    procedure varchar,
    commands varchar,
    status varchar,
    error varchar,
    user varchar,
    role varchar,
    task varchar,
    start_dtm timestamp,
    end_dtm timestamp,
    duration number,
    insert_dtm timestamp default current_timestamp
);

use role securityadmin;

grant usage on database {{ database }} to role securityadmin;
grant usage on schema {{ schema }} to role securityadmin;
grant select, insert on table {{ schema }}.procedure_logs to role securityadmin;