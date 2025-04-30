#!/bin/bash

for i in $(seq $1 $2) 
do
    ssh X1aoyang@node$i "cd ~/FineMem; git pull; rm -rf ./build; mkdir build; cd build; cmake ..; make "
done