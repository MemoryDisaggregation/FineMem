#!/bin/bash

printf "Allocation size(4KB), FineMem, Premmap-One-sided, Premmap-RPC\n" > different_size.csv

printf "1, " >> different_size.csv
./start_microbench.sh 1 8 16 "pool" 0 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 0 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 0 "different_size.csv"
printf "\n2, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 1 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 1 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 1 "different_size.csv"
printf "\n4, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 2 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 2 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 2 "different_size.csv"
printf "\n8, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 3 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 3 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 3 "different_size.csv"
printf "\n16, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 4 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 4 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 4 "different_size.csv"
printf "\n32, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 5 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 5 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 5 "different_size.csv"
printf "\n64, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 6 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 6 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 6 "different_size.csv"
printf "\n128, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 7 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 7 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 7 "different_size.csv"
printf "\n256, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 8 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 8 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 8 "different_size.csv"
printf "\n512, " >> different_size.csv

./start_microbench.sh 1 8 16 "pool" 9 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "cxl" 9 "different_size.csv"
printf ", " >> different_size.csv
./start_microbench.sh 1 8 16 "fusee" 9 "different_size.csv"
printf "\n" >> different_size.csv