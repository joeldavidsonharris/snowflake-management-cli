use role securityadmin;

grant imported privileges on database snowflake to role sysadmin;

use role sysadmin;
use warehouse admin_whs;

create schema if not exists {{ schema }};

create or replace procedure {{ schema }}.sp_log_account_usage("reader_objects" varchar)
returns varchar not null
language javascript
execute as owner
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
    
    // Set variables
    var table_names = ['login_history', 'query_history', 'access_history'];
    var highwater_columns = ['event_timestamp', 'start_time', 'query_start_time'];
    
    var reader_table_names = ['login_history', 'query_history'];
    var reader_highwater_columns = ['event_timestamp', 'start_time'];
    
    // If tables dont exist, create them
    for (var i = 0; i < table_names.length; i++) {
        execute_sql(`
            create table if not exists {{ schema }}.${table_names[i]} as
            select * from snowflake.account_usage.${table_names[i]}
        `);
    }
    
    if (reader_objects == 'true') {
        for (var i = 0; i < reader_table_names.length; i++) {
            execute_sql(`
                create table if not exists {{ schema }}.reader_${reader_table_names[i]} as
                select * from snowflake.reader_account_usage.${reader_table_names[i]}
            `);
        }
    }
    
    // Get high water marks from existing tables
    // Select data from account_usage which is after high watermarks
    // Insert into archive tables
    for (var i = 0; i < table_names.length; i++) {
        execute_sql(`
            insert into {{ schema }}.${table_names[i]}
            select * 
            from snowflake.account_usage.${table_names[i]}
            where ${highwater_columns[i]} > (
                select max(${highwater_columns[i]})
                from {{ schema }}.${table_names[i]}
            )
        `);
    }
    
    if (reader_objects == 'true') {
        for (var i = 0; i < reader_table_names.length; i++) {
            execute_sql(`
                insert into {{ schema }}.reader_${reader_table_names[i]}
                select * 
                from snowflake.reader_account_usage.${reader_table_names[i]}
                where ${reader_highwater_columns[i]} > (
                    select max(${reader_highwater_columns[i]})
                    from {{ schema }}.reader_${reader_table_names[i]}
                )
            `);
        }
	}
    
    /*------------ Add code above ------------*/
    
}

catch (err) {
	log_procedure_run(commands_run, 'Failure', err, start_time);
    return err;
}

log_procedure_run(commands_run, 'Success', null, start_time);
return 'Success';

$$;

call {{ schema }}.sp_log_account_usage('{{ reader }}');

create or replace task {{ schema }}.tsk_log_account_usage
warehouse = admin_whs
schedule = 'USING CRON 0 0 * * * NZ'
as
call {{ schema }}.sp_log_account_usage('{{ reader }}');

alter task {{ schema }}.tsk_log_account_usage resume;