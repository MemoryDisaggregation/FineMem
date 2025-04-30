#!/bin/bash

ssh X1aoyang@node1 "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 pool frag 2 >/dev/null 2>&1 &"

ssh X1aoyang@node2 "~/FineMem/build/microbench/v_microbench 10.10.1.1 1111 16 0 pool frag 2 >/dev/null 2>&1 &"
