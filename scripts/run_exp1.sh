#!/bin/bash

printf "Allocation size(4KB), FineMem, Premmap-One-sided, Premmap-RPC\n" > different_size.csv

printf "1, " >> different_size.csv

./start_microbench.sh 1 6 "pool" 0 "different_size.csv"

printf ", " >> different_size.csv

./start_microbench.sh 1 6 "cxl" 0 "different_size.csv"

printf ", " >> different_size.csv

./start_microbench.sh 1 6 "fusee" 0 "different_size.csv"

printf "\n" >> different_size.csv
