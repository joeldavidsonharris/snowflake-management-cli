use role sysadmin;
use warehouse admin_whs;

create schema if not exists {{ schema }};

create or replace procedure {{ schema }}.sp_disable_inactive_users("inactive_days" varchar)
returns varchar not null
language javascript
execute as caller
as
$$

var start_time = execute_sql('select current_timestamp, current_timestamp::varchar', null, true, true);
var commands_run = [];

function execute_sql(sql, binds, next, audit=false) {
	var stmt = snowflake.createStatement({sqlText: sql, binds: binds});
	try {
		var result = stmt.execute();
	} catch (err) {
        var query_id = execute_sql('select last_query_id()', null, true, true).getColumnValue(1);
		commands_run.push({command: sql, query_id: query_id, result: 'Failure'});
		throw `Failure executing command:\n Query ID: '${query_id}'\nSQL: \n${sql}`;
	}
	if (next === true) {
		result.next();
	}
	if (audit === false) {
		commands_run.push({command: sql, query_id: result.getQueryId(), result: 'Success'});
	}
	return result;
}

function log_procedure_run(commands_run, status, error, start_time) {
	var end_time = execute_sql('select current_timestamp, current_timestamp::varchar', null, true, true);
	var duration = end_time.getColumnValue(1).getEpochSeconds() - start_time.getColumnValue(1).getEpochSeconds();
	execute_sql(`
		insert into {{ logging_schema }}.procedure_logs values (
			lower('{{ schema }}' || '.' || '${Object.keys(this)[0]}'), 
			?, 
			'${status}',
			?,
            lower(current_user),
            lower(current_role()),
            nullif(lower(system$current_user_task_name()), ''),
			'${start_time.getColumnValue(2)}'::timestamp_ltz, 
			'${end_time.getColumnValue(2)}'::timestamp_ltz, 
			${duration}::number,
			current_timestamp
		)`,
		[JSON.stringify(commands_run), (!error ? error : error.toString())],
		false,
		true
	);
}

try {

    /*------------ Add code below ------------*/
 
    var users = execute_sql('show users');
    
    // Find and disable all eligible users that havent logged in within inactive_days days
	var inactive_users = execute_sql(`
        select distinct "name"
        from table(result_scan('${users.getQueryId()}'))
        where "name" not ilike '%_admin'
            and not regexp_like("name", '^SA_[a-zA-Z0-9_@.]+')
            and "disabled" ilike 'false'
            and datediff(day, ifnull("last_success_login", "created_on"), current_timestamp) > ${inactive_days}
    `);
    
    while (inactive_users.next()) {
        var user_name = inactive_users.getColumnValue('name');
        execute_sql(`alter user if exists "${user_name}" set disabled = true`);
    }
	
    /*------------ Add code above ------------*/
    
}

catch (err) {
	log_procedure_run(commands_run, 'Failure', err, start_time);
    return 'Failure';
}

log_procedure_run(commands_run, 'Success', null, start_time);
return 'Success';

$$;

use role securityadmin;

grant usage on database {{ database }} to role securityadmin;
grant usage, create task on schema {{ schema }} to role securityadmin;
grant usage on procedure {{ schema }}.sp_disable_inactive_users(varchar) to role securityadmin;

call {{ schema }}.sp_disable_inactive_users('{{ inactive_days }}');

create or replace task {{ schema }}.tsk_disable_inactive_users
warehouse = admin_whs
schedule = 'USING CRON 0 0 * * * NZ'
as
call {{ schema }}.sp_disable_inactive_users('{{ inactive_days }}');

alter task {{ schema }}.tsk_disable_inactive_users resume;