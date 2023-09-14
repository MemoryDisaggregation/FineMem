/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-12 22:24:28
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-08-14 17:34:13
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
    while(iter--){
      heap->fetch_cache(iter, addr, rkey);
      std::cout << "addr: " << std::hex << addr << " rkey: " << rkey << std::endl;
    }
    getchar();
    heap->stop();
    delete heap;
    return 0;

}