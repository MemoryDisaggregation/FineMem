/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-09-13 22:28:40
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-15 16:23:51
 * @FilePath: /rmalloc_newbase/source/user_code.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <cstdio>
#include "cpu_cache.h"
#include "rdma_conn_manager.h"

const uint64_t cache_size = 1024*1024*2;

int main(int argc, char** argv){
    mralloc::cpu_cache cpu_cache_ = mralloc::cpu_cache(cache_size);
    mralloc::ConnectionManager* m_rdma_conn_ = new mralloc::ConnectionManager();
    // if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_ == nullptr || m_rdma_conn_->init("10.0.0.63", "1145", 2, 20) == -1 ){
        printf("rdma connection create failed!\n");
    }
    int k=1;
    uint64_t addr; uint32_t rkey;
    char buffer1[5] = "aaaa";
    char buffer2[5] = "bbbb";
    char read_buffer[5];
    while(k--){
        cpu_cache_.fetch_cache(1, addr, rkey);
        m_rdma_conn_->remote_write(buffer1, 5, addr, rkey);
        m_rdma_conn_->remote_read(read_buffer, 5, addr, rkey);
        printf("alloc: %lx : %u, content: %s\n", addr, rkey, read_buffer);
        // rkey should be used by server side, client side has no necessary to support rkey
        // uint32_t newkey = ibv_inc_rkey(rkey);
        // m_rdma_conn_->remote_mw(addr, rkey, cache_size, newkey);
        // m_rdma_conn_->remote_write(buffer2, 5, addr + cache_size/2, newkey);
        // m_rdma_conn_->remote_read(read_buffer, 5, addr + cache_size/2, newkey);
        // printf("using new key: %lx : %u, content: %s\n", addr, newkey, read_buffer);
        // m_rdma_conn_->remote_write(buffer1, 5, addr, rkey);
        // m_rdma_conn_->remote_read(read_buffer, 5, addr, rkey);
        // printf("using old key: %lx : %u, content: %s\n", addr, rkey, read_buffer);
    }
    return 0;
}