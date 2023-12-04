/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-12-04 14:32:55
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-04 14:47:21
 * @FilePath: /rmalloc_newbase/include/mr_utils.h
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

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