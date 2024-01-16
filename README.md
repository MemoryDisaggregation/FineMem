# LegoAlloc - Main Repo

🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧This repo is under constructing🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧

All source codes of LegoHeap and FuDM are avaliable here, while we are still working on complete the documents. And the source code of LegoAlloc-User, LegoAlloc-Swap and LegoAlloc-KV stores at other repos, which we will add in this repo future.

Only OFED & Cmake needed, no other libraray requirement.

Recommendate running on Cloudlab r650/r6525/c6525

* If use libmralloc.a, the following libraries are usable:

  ./include/cpu_cache.h: The cpu cache interface (C++)

  ./include/msg.h: the RDMA connection settings/RPC packet content defines
  
  ./include/rdma_conn.h: A single RDMA connector
  
  ./include/rdma_conn_manager.h: A multiple RDMA connectors manager
  
  ./include/free_block_magnaer.h: The remote metadata interface
  


* Important source files:
  
  ./source/computing_node.cc: Mutitenant shared memory pool, filler and pre-fetcher
  
  ./source/memory_node.cc: Init and pre-registarion, rkey generate, RPC execution
  
  ./source/free_block_manager.cc: The FuDM metadata and local version FuDM funtion
  
  ./source/rdma_conn.cc: FuDM protocol


* Others:
  
  ./scripst: Set the memory node's hugepage number; Install the OFED Driver on cloudlab nodes(both for intel and amd)
  
  ./microbench: Microbench code, microbench_class.cc & microbench_common.cc

