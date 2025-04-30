#!/bin/bash

ps aux | grep "redis-server 10.10.1.1:2222" | awk '{print $2}' | xargs kill

ps aux | grep "server mlx5_3 10.10.1.1 1111" | awk '{print $2}' | xargs kill

