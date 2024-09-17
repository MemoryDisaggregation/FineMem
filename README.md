# FineMem - Main Repo

> All source codes of FineMem are avaliable here, while we are still working on complete the documents. And the source code of FineMem-User, FineMem-Swap and FineMem-KV stores at other repos, which we will refer in this repo future.

> I'm not an expert of C++ or RDMA, but I'll try my best to help anyone who wants to build and run this system. And I'm glad to see any advise or comments about this project. Wish one day we can see DM's large scale deployment on actual cloud enviroments.

🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧This repo is still under constructing🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧🚧

## Build 


* Only `OFED` & `Cmake` needed, no other libraray requirement.
    * `scripts/install_ofed.sh` can help you quickly build up OFED enviroment on a new cloudlab node

* Recommendate running on `Cloudlab r650/r6525/c6525/d6515

```shell
> sudo ./scripts/install_ofed.sh
> mkdir build; cd build
> cmake ..; make 
```

## Run

### Memory-side FineMem:

using `ibdev2netdev` to show which RNIC(with status UP) you can use.

```
> sudo ./scripts/set_2MB_hugepage.sh 200000
> cd ./build/source
> ./server mlx_2<available RDMA device name> 10.10.1.1<server RDMA IP> 1234<serverport>
```


### Compute-side LegoHeap:

```
> cd ./build/source
> ./client 10.10.1.1<server RDMA IP> 1234<server port>
```

### Compute-side Microbench:

```
> cd ./build/microbench
> ./microbench_common 10.10.1.1<server RDMA IP> 1234<server port> 48<thread number> <the allocator type from "cxl", "fusee", "share"(FineMem Raw), "pool"(FineMem with Service), "exclusive"> <the benchmark from "shuffle" and "stage">
```

## Code content

* If use libmralloc.a, the following libraries are usable:

  ./include/cpu_cache.h: The cpu cache interface (C++)

  ./include/msg.h: the RDMA connection settings/RPC packet content defines
  
  ./include/rdma_conn.h: FuDM communication
  
  ./include/rdma_conn_manager.h: FuDM communication manager
  
  ./include/free_block_magnaer.h: FuDM metadata
  


* Important source files:
  
  ./source/computing_node.cc: LegoHeap
  
  ./source/memory_node.cc: FineMem memory server 

  ./source/free_block_manager.cc: FuDM metadata 

  ./source/rdma_conn.cc: FuDM communication


* Others:
  
  ./scripst: Set the memory node's hugepage number; Install the OFED Driver on cloudlab nodes(both for intel and amd)
  
  ./microbench: Microbench code, microbench_class.cc & microbench_common.cc

