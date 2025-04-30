#!/bin/bash

sudo echo 16000 > /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

nohup redis-server --bind 10.10.1.1 --port 2222 --protected-mode no