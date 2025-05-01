# FineMem - Main Repo

FineMem is a distributed remote memory management system designed to support fine-grained, high-performance memory (de)allocation in RDMA-connected DM. This ReadMe has four part, first is how to build FineMem, then an easy case to run FineMem, following the content explaniation and experiments reproduction.

## Build 

* Recommendate running on `Cloudlab r650/r6525/c6525/d6515/xl170` 

* OS & Driver version
  
  * Ubuntu 22.04/Ubuntu 20.04

  * MLNX_OFED 5.x/4.x

* All nodes need install drivers and libraries as following commands:

```shell
> git clone https://github.com/MemoryDisaggregation/FineMem.git
> cd FineMem/
> sudo ./scripts/env_setup.sh # Warning: node will reboot here
> mkdir build; cd build
> cmake ..; make 
```

## Hello-World Example

### Memory-side FineMem:

using `ibdev2netdev` to show which RNIC(with status UP) you can use.

```shell
> sudo ./scripts/set_2MB_hugepage.sh 200000 
> cd ./build/source
> ./server mlx_3<available RDMA device name> 10.10.1.1<server RDMA IP> 1234<serverport>
```


### Compute-side FineMem service:

```shell
> sudo ./scripts/set_2MB_hugepage.sh 2000 
> cd ./build/source
> ./client ../../config/config.json
```

```json
client-side config file:
{
    "node_id":1,
    "rdma_cm_port":1111,
    "memory_node_num":1,
    "memory_ips":[
        "10.10.1.1"
    ]
}
```

### Compute-side Microbench:

```
> cd ./build/microbench
> ./microbench_common 10.10.1.1 1234 16<thread number> 0<size 0-9> pool<allocator type from "cxl", "fusee", "pool">, 1<total node number>
```

## Code content

### Interfaces

When using libmralloc.a, the following libraries are notable:

* ./include/cpu_cache.h: semaphore-based message transfer interface between user applications and server applications.

* ./include/msg.h: RDMA connection settings and RPC packet content defininations
  
* ./include/rdma_conn.h: FineMem communication and raw allocation transaction interfaces
  
* ./include/rdma_conn_manager.h: FineMem communication group manager interfaces
  
* ./include/free_block_magnaer.h: FineMem metadata management and recovery interfaces
  

### Source Code

Core source files to better understand the implementation:
  
* ./source/computing_node.cc: FineMem compute node service
  
* ./source/memory_node.cc: FineMem memory server 

* ./source/free_block_manager.cc: FineMem metadata management and recovery

* ./source/rdma_conn.cc: FineMem communication and allocation transaction



## Evaluation Reproducing

### Microbench

```shell
> cd FineMem/scripts/microbench
> ./run_different_node.sh
> ./run_different_size_16.sh
> ./run_different_size_128.sh
> ./run_different_thread.sh
> python3 size_alloc.py
> python3 thread_alloc.py
```
### DM KV-Store System

```shell
> cd FineMem/scripts/
> ./batch_build_fusee.sh
> ./run_fusee_2MB.sh
> ./run_fusee_4KB.sh
```

### DM Swap System

See Readme in folder Swap_FinrMem_Sim
