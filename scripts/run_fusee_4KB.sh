#!/bin/bash

printf "Client Number, FineMem, Premmap-One-sided, Premmap-RPC\n" > fusee_4kb.csv

printf "16, " >> fusee_4kb.csv
./fusee_bench.sh 1 2 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 2 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 2 "fusee" "fusee_4kb.csv" 4096 10
printf "\n32, " >> fusee_4kb.csv

./fusee_bench.sh 1 4 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 4 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 4 "fusee" "fusee_4kb.csv" 4096 10
printf "\n48, " >> fusee_4kb.csv

./fusee_bench.sh 1 6 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 6 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 6 "fusee" "fusee_4kb.csv" 4096 10
printf "\n64, " >> fusee_4kb.csv

./fusee_bench.sh 1 8 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 8 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 8 "fusee" "fusee_4kb.csv" 4096 10
printf "\n80, " >> fusee_4kb.csv

./fusee_bench.sh 1 10 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 10 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 10 "fusee" "fusee_4kb.csv" 4096 10
printf "\n96, " >> fusee_4kb.csv

./fusee_bench.sh 1 12 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 12 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 12 "fusee" "fusee_4kb.csv" 4096 10
printf "\n112, " >> fusee_4kb.csv

./fusee_bench.sh 1 14 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 14 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 14 "fusee" "fusee_4kb.csv" 4096 10
printf "\n128, " >> fusee_4kb.csv

./fusee_bench.sh 1 16 "pool" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 16 "cxl" "fusee_4kb.csv" 4096 10
printf ", " >> fusee_4kb.csv
./fusee_bench.sh 1 16 "fusee" "fusee_4kb.csv" 4096 10
