/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-12 22:24:28
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-15 16:23:58
 * @FilePath: /rmalloc_newbase/source/client.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <cstdio>
#include <string>
#include <iostream>
#include "kv_engine.h"
#include "memory_heap.h"

int main(int argc, char* argv[]){

    if(argc < 3){
        printf("Usage: %s <ip> <port>\n", argv[0]);
        return 0;
    }

    std::string ip = argv[1];
    std::string port = argv[2];

    mralloc::LocalHeap* heap = new mralloc::LocalHeap();
    heap->start(ip, port);

    // fetch remote memory
    int iter = 10;
    uint64_t addr;
    uint32_t rkey=0;
    char buffer[2][64*1024] = {"aaa", "bbb"};
    char read_buffer[4];
    while(iter--){
        heap->fetch_mem_fast_remote(addr, rkey);
        std::cout << "write addr: " << std::hex << addr << " rkey: " << std::dec <<rkey << std::endl;
        for(int i = 0; i < 1024; i++)
            heap->get_conn()->remote_write(buffer[iter%2], 64*1024, addr+i*64*1024, rkey);
        std::cout << "read addr: " << std::hex << addr << " rkey: " << std::dec <<rkey << std::endl;
        for(int i = 0; i < 1024; i++)
            heap->get_conn()->remote_read(read_buffer, 4, addr, rkey);
        printf("alloc: %lx : %u, content: %s\n", addr, rkey, read_buffer);
      // heap->mr_bind_remote(2*1024*1024, addr, rkey, 114514);
      // std::cout << "addr mw bind success " << std::endl;
    }
    getchar();
    heap->stop();
    delete heap;
    return 0;

}