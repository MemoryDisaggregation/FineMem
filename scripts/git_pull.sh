#!/bin/bash

for i in $(seq $1 $2) 
do
    ssh X1aoyang@node$i "mkdir ~/FineMem/build; cd ~/FineMem/build; git stash; git checkout revision; git pull; cmake ..; make -j"
done