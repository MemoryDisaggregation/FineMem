#!/bin/bash

for i in $(seq $1 $2) 
do
    ssh X1aoyang@node$i "cd ~/FineMem/build; git pull; make "
done