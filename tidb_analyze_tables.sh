#!/bin/bash

tidb_host="127.0.0.1"
tidb_port=4000
tidb_user="root"

healthy_threshold=0.5

date

for tidbschema in $(mysql -h$tidb_host -P$tidb_port -u$tidb_user -Nse "select schema_name from information_schema.schemata where schema_name not in ('INFORMATION_SCHEMA', 'METRICS_SCHEMA', 'PERFORMANCE_SCHEMA', 'mysql');")
do
    echo "tidbschema:$tidbschema"
    for tidbtable in $(mysql -h$tidb_host -P$tidb_port -u"$tidb_user" -D"$tidbschema"  -Nse "show tables")
    do
        echo "tidbtable:$tidbtable"
        need_analyze=0
        analyzeglobal=0
        partitions=""
        while read -a row
        do
            echo "db_name:${row[0]}, table_name:${row[1]}, partition_name:${row[2]}, healthy:${row[3]}"
            if [[ $(echo ${row[3]} < $healthy_threshold | bc) -eq 1 ]]; then
                need_analyze=1
                if [[ -n "${row[2]}" ]]; then
                    if [ "${row[2]}" = "global" ]; then
                        analyzeglobal=1
                    else
                        if [[ -n "$partitions" ]]; then
                            partitions="$partitions" ",${row[2]}"
                        else
                            partitions="\`${row[2]}\`"
                        fi
                    fi
                fi
            fi
        done< <(mysql -h$tidb_host -P$tidb_port -u"$tidb_user" -D"$tidbschema"  -Nse "show stats_healthy where db_name = \"$tidbschema\" and table_name = \"$tidbtable\"")
        if [[ $need_analyze -eq 0 ]]; then
            echo "table $tidbschema.$tidbtable doesn't need analyze, skip"
            continue
        fi
        is_analyzing=$(mysql -h$tidb_host -P$tidb_port -u"$tidb_user" -D"$tidbschema"  -Nse "select count(1) from mysql.analyze_jobs where table_schema = \"$tidbschema\" and table_name = \"$tidbtable\" and state in ('pending', 'running') and update_time > current_timestamp() - interval 6 hour")
        if [[ ${{is_analyzing}} -gt 0 ]]; then
            echo "table $tidbschema.$tidbtable is being analyzed, skip"
            continue
        fi
        analyze_command="setanalyze table"
        "select count(1) from mysql.analyze_jobs where table_schema = 'test' and table_name = 't' and state in ('pending', 'running') and update_time > current_timestamp() - interval 6 hour"
        mysql -h$tidb_host -P"$tidb_port" -u"$tidb_user" -D"$tidbschema" -e "analyze table \`$tidbtable\`;" 
        echo "`date` | \`$tidbschema\`.\`$tidbtable\` analyze done"
    done
done

date
