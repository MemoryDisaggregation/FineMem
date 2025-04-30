#!/bin/bash

redis-cli -h 10.10.1.1 -p 2222 SET bench_start 0

ps aux | grep "redis-server 10.10.1.1:2222" | awk '{print $2}' | xargs kill

ps aux | grep "server mlx5_3 10.10.1.1 1111" | awk '{print $2}' | xargs kill

