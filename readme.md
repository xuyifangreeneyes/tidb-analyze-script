```
chmod +x /root/tidb_analyze_partition_tables.sh
```

Add a new crontab job.
```
crontab -u root -e
```

Trigger the manual analyze script every day at 18:00.
```
0 18 * * * /root/tidb_analyze_partition_tables.sh &>> /root/tidb_analyze_partition_tables.log
```
