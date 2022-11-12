#!/bin/bash

tidb_host="127.0.0.1"
tidb_port=4000
tidb_user="root"

healthy_threshold=0.5
tidb_build_stats_concurrency=1
tidb_distsql_scan_concurrency=1
analyze_max_execution_time=0

echo "`date` | start analyze tables"

for schema in $(mysql -h$tidb_host -P$tidb_port -u$tidb_user -Nse "select schema_name from information_schema.schemata where schema_name not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql');")
do
    echo "schema:$schema"
    for table in $(mysql -h$tidb_host -P$tidb_port -u$tidb_user -D$schema  -Nse "show tables")
    do
        echo "table:$table"
        need_analyze=0
        analyze_global=0
        partitions=""
        while read -a row
        do
            echo "${row[@]}"
            if [[ ${#row[@]} -eq 3 ]]; then
                if [[ $(echo "${row[2]} < $healthy_threshold" | bc) -eq 1 ]]; then
                    need_analyze=1
                fi
            else
                if [[ $(echo "${row[3]} < $healthy_threshold" | bc) -eq 1 ]]; then
                    need_analyze=1 
                    if [ "${row[2]}" = "global" ]; then
                        analyze_global=1
                    else
                        if [[ -n "$partitions" ]]; then
                            partitions="$partitions" ",${row[2]}"
                        else
                            partitions="\`${row[2]}\`"
                        fi
                    fi
                fi
            fi
        done< <(mysql -h$tidb_host -P$tidb_port -u$tidb_user -D$schema  -Nse "show stats_healthy where db_name = \"$schema\" and table_name = \"$table\"")
        if [[ $need_analyze -eq 0 ]]; then
            echo "table $schema.$table doesn't need analyze, skip"
            continue
        fi
        is_analyzing=$(mysql -h$tidb_host -P$tidb_port -u$tidb_user -D$schema  -Nse "select count(1) from mysql.analyze_jobs where table_schema = \"$schema\" and table_name = \"$table\" and state in ('pending', 'running') and update_time > current_timestamp() - interval 6 hour")
        if [[ ${{is_analyzing}} -gt 0 ]]; then
            echo "table $schema.$table is being analyzed, skip"
            continue
        fi
        analyze_command="setanalyze table \`$schema\`.\`$table\`"
        if [[ -n "$partition" ]] && [[ analyze_global -eq 0 ]]; then
            analyze_command="$analyze_command" " partition $partitions"
        fi
        echo "`date` | $analyze_command start"
        mysql -h$tidb_host -P$tidb_port -u$tidb_user -D$schema -e "set @@session.tidb_build_stats_concurrency=$tidb_build_stats_concurrency;set @@session.tidb_distsql_scan_concurrency=$tidb_distsql_scan_concurrency;set @@session.max_execution_time=$analyze_max_execution_time;$analyze_command" 
        echo "`date` | $analyze_command done"
    done
done

echo "`date` | finish analyze tables"

