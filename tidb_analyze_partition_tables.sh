#!/bin/bash

tidb_host="127.0.0.1"
tidb_port=4000
tidb_user="root"
tidb_password="password"

healthy_threshold=10
tidb_build_stats_concurrency=1
tidb_distsql_scan_concurrency=1
analyze_max_execution_time=0 # unit: second
analyze_window_time=43200 # unit: second

echo "`date` | start analyze tables"
start_time=$(date +%s)

for schema in $(mysql -h$tidb_host -P$tidb_port -u$tidb_user -p$tidb_password -Nse "select schema_name from information_schema.schemata where schema_name not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql');")
do
    echo "schema:$schema"
    for table in $(mysql -h$tidb_host -P$tidb_port -u$tidb_user -p$tidb_password -D$schema  -Nse "show tables")
    do
        now_time=$(date +%s)
        duration=$(( now_time - start_time ))
        if [[ duration -gt analyze_window_time ]]; then
            echo "duration:$duration, reach analyze_window_time"
            break 2
        fi
        echo "table:$table"
        partitions=""
        while read -a row
        do
            echo "${row[@]}"
            if [[ ${#row[@]} -eq 4 ]] && [ "${row[2]}" != "global" ] && [[ $(echo "${row[3]} < $healthy_threshold" | bc) -eq 1 ]]; then
                if [[ -n "$partitions" ]]; then
                    partitions="$partitions,${row[2]}"
                else
                    partitions="${row[2]}"
                fi
            fi
        done< <(mysql -h$tidb_host -P$tidb_port -u$tidb_user -p$tidb_password -D$schema  -Nse "show stats_healthy where db_name = \"$schema\" and table_name = \"$table\"")
        if [[ -z "$partitions" ]]; then
            echo "skip table $schema.$table"
            continue
        fi
        is_analyzing=$(mysql -h$tidb_host -P$tidb_port -u$tidb_user -p$tidb_password -D$schema  -Nse "select count(1) from mysql.analyze_jobs where table_schema = \"$schema\" and table_name = \"$table\" and state in ('pending', 'running') and update_time > current_timestamp() - interval 6 hour")
        if [[ is_analyzing -gt 0 ]]; then
            echo "table $schema.$table is being analyzed, skip"
            continue
        fi
        analyze_command="analyze table $schema.$table partition $partitions"
        echo "`date` | $analyze_command start"
        mysql -h$tidb_host -P$tidb_port -u$tidb_user -p$tidb_password -D$schema -e "set @@session.tidb_build_stats_concurrency=$tidb_build_stats_concurrency;set @@session.tidb_distsql_scan_concurrency=$tidb_distsql_scan_concurrency;set @@session.max_execution_time=$analyze_max_execution_time;$analyze_command" 
        echo "`date` | $analyze_command done"
    done
done

echo "`date` | finish analyze tables"

