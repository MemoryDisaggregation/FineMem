#!/bin/bash

printf "Client Number, FineMem, Premmap-One-sided, Premmap-RPC\n" > fusee_2mb.csv

printf "16, " >> fusee_2mb.csv
./fusee_bench.sh 1 1 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 1 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 1 "fusee" "fusee_2mb.csv" 2097152
printf "\n32, " >> fusee_2mb.csv

./fusee_bench.sh 1 2 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 2 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 2 "fusee" "fusee_2mb.csv" 2097152
printf "\n48, " >> fusee_2mb.csv

./fusee_bench.sh 1 3 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 3 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 3 "fusee" "fusee_2mb.csv" 2097152
printf "\n64, " >> fusee_2mb.csv

./fusee_bench.sh 1 4 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 4 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 4 "fusee" "fusee_2mb.csv" 2097152
printf "\n80, " >> fusee_2mb.csv

./fusee_bench.sh 1 5 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 5 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 5 "fusee" "fusee_2mb.csv" 2097152
printf "\n96, " >> fusee_2mb.csv

./fusee_bench.sh 1 6 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 6 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 6 "fusee" "fusee_2mb.csv" 2097152
printf "\n112, " >> fusee_2mb.csv

./fusee_bench.sh 1 7 "pool" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 7 "cxl" "fusee_2mb.csv" 2097152
printf ", " >> fusee_2mb.csv
./fusee_bench.sh 1 7 "fusee" "fusee_2mb.csv" 2097152
printf "\n128, " >> fusee_2mb.csv
