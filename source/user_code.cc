/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-09-13 22:28:40
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-14 15:41:53
 * @FilePath: /rmalloc_newbase/source/user_code.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/stdint-uintn.h>
#include <cstdio>
#include "cpu_cache.h"
#include "rdma_conn_manager.h"

const uint64_t cache_size = 1024*1024*2;

int main(int argc, char** argv){
    mralloc::cpu_cache cpu_cache_ = mralloc::cpu_cache(cache_size);
    mralloc::ConnectionManager* m_rdma_conn_ = new mralloc::ConnectionManager();
    if (m_rdma_conn_ == nullptr) return -1;
    m_rdma_conn_->init("10.0.0.63", "1145", 4, 20);
    int k=1000;
    uint64_t addr; uint32_t rkey;
    while(k--){
        cpu_cache_.fetch_cache(1, addr, rkey);
        printf("alloc: %lx : %u\n", addr, rkey);
    }
    return 0;
}