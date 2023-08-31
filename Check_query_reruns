CREATE OR REPLACE PROCEDURE PROC_CHECK_RERUNS(X STRING, USERNAME STRING )
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$ 

   // Step 1. Identify long running query. This checks for query running for more than X minutes for last 1 week for the input username. 
    var longRunningQueries = snowflake.execute({sqlText: `SELECT query_id, query_tag, WAREHOUSE_NAME, total_elapsed_time, start_time FROM snowflake.account_usage.query_history WHERE total_elapsed_time > ${X * 60 * 1000} AND user_name = '${USERNAME}'
    and end_time::date > CURRENT_DATE -7  and query_type <> 'CALL' `});
    var result = [];
    while (longRunningQueries.next()) {
        var queryId = longRunningQueries.getColumnValue(1);
        var queryTag = longRunningQueries.getColumnValue(2);
        var warehouse = longRunningQueries.getColumnValue(3);
        var elapsedTime = longRunningQueries.getColumnValue(4);
        var startTime = longRunningQueries.getColumnValue(5);

        // Check against get_query_operator_stats. If step_id > 1000 then it means snowflake attempted a rerun. 
        var operatorStats = snowflake.execute({sqlText: `SELECT step_id FROM table(get_query_operator_stats('${queryId}'))
           where step_id > 1000`});
        while (operatorStats.next()) {
            var stepId = operatorStats.getColumnValue(1);
            if (stepId > 1000) {
                result.push({
                    query_id: queryId,
                    query_tag: queryTag,
                    warehouse: warehouse,
                    elapsed_seconds: (elapsedTime / 1000).toFixed(2),
                    start_time: startTime,
                    USERNAME: USERNAME
                });
                break;
            }
        }
    }
    return result;

$$;


// Example 
// CALL PROC_CHECK_RERUNS('12', 'DBT_USER'); 
//SELECT 
//    value:query_id::string as query_id, 
//    value:query_tag::string as query_tag, 
//    value:elapsed_seconds::int/60 as query_dur_mins, 
//    value:start_time::timestamp_ltz as query_starttime,
//    value:warehouse::string as warehouse
//From table(result_scan(last_query_id())) as x , 
//lateral flatten (proc_check_reruns)
