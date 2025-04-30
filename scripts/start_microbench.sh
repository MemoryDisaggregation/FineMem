#!/bin/bash

./stop_remote_client.sh $1 $2

./stop_server.sh

./start_server.sh

sleep 5

./start_remote_client.sh $1 $2

sleep 10

redis-cli -h 10.10.1.1 -p 2222 SET bench_start 0 >/dev/null 2>&1
redis-cli -h 10.10.1.1 -p 2222 SET avg 0 >/dev/null 2>&1
redis-cli -h 10.10.1.1 -p 2222 SET finished 0 >/dev/null 2>&1

node_num=$(echo "$2 - $1 + 1" | bc)

for i in $(seq $1 $2) 
do
    # ssh X1aoyang@node$i "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 pool frag $i >/dev/null 2>&1 &"
    # ssh X1aoyang@node$i "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 cxl frag $i >/dev/null 2>&1 &"
    ssh X1aoyang@node$i "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 $4 $3 frag $node_num >/dev/null 2>&1 &"
done

while true; do
    start_server=$( redis-cli -h 10.10.1.1 -p 2222 GET "bench_start" )
    current_value=$( redis-cli -h 10.10.1.1 -p 2222 GET "finished" )
    echo "Start Server: $start_server, Current value: $current_value, Target: $node_num"
    if [ "$current_value" = "$node_num" ]; then
        # echo "Value matched. Exiting."
        break
    fi

    sleep 1
done

average1=$( echo "scale=3; $(redis-cli -h 10.10.1.1 -p 2222 GET avg) / $node_num" | bc )
echo $average1 >> $5

./stop_remote_client.sh $1 $2

./stop_server.sh 