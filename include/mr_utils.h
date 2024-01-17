
#include <bits/stdint-uintn.h>
namespace mralloc {

struct GlobalConfig {

    uint32_t server_id;
    uint16_t rdma_cm_port;
    uint32_t memory_node_num; 
    char     memory_ips[16][16];

    uint64_t mem_pool_base_addr;
    uint64_t mem_pool_size;
    uint64_t block_size;

};

}