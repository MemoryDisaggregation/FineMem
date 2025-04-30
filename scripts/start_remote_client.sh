#!/bin/bash

ssh X1aoyang@node1 "sudo ~/FineMem/scripts/set_2MB_hugepage.sh 10000"

ssh X1aoyang@node2 "sudo ~/FineMem/scripts/set_2MB_hugepage.sh 10000"

ssh X1aoyang@node1 "~/FineMem/build/source/client ~/FineMem/config/config.json >/dev/null 2>&1 &"

ssh X1aoyang@node2 "~/FineMem/build/source/client ~/FineMem/config/config.json >/dev/null 2>&1 &"