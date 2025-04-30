#!/bin/bash

redis-cli -h 10.10.1.1 -p 2222 SET bench_start 0 >/dev/null 2>&1
redis-cli -h 10.10.1.1 -p 2222 SET avg 0 >/dev/null 2>&1
redis-cli -h 10.10.1.1 -p 2222 SET finished 0 >/dev/null 2>&1

for i in $(seq 1 $1) 
do
    # ssh X1aoyang@node$i "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 pool frag $i >/dev/null 2>&1 &"
    # ssh X1aoyang@node$i "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 cxl frag $i >/dev/null 2>&1 &"
    ssh X1aoyang@node$i "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 fusee frag $i >/dev/null 2>&1 &"
done

while true; do
    current_value=$( redis-cli -h 10.10.1.1 -p 2222 GET "finished" )
    # echo "Current value: $current_value, Target: $1"
    if [ "$current_value" = "$1" ]; then
        # echo "Value matched. Exiting."
        break
    fi

    sleep 1
done

# echo $(redis-cli -h 10.10.1.1 -p 2222 GET finished)
# echo $(redis-cli -h 10.10.1.1 -p 2222 GET avg)
average1=$(echo "scale=3; $(redis-cli -h 10.10.1.1 -p 2222 GET avg) / $1" | bc)
echo $average1