/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:21:21
 * @LastEditors: blahaj wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-06 22:41:54
 * @FilePath: /rmalloc_newbase/source/server.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <cstdio>
#include <string>
#include <iostream>
// #include "memory_heap.h"
#include "memory_node.h"
// #include <gperftools/profiler.h>

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << "<device> <ip> <port>" << std::endl;
    return 0;
  }

  std::string device = argv[1];
  std::string ip = argv[2];
  std::string port = argv[3];
//   ProfilerStart("rmalloc.prof");
  mralloc::MemoryNode *heap = new mralloc::MemoryNode(true);
  heap->start(ip, port, device);

  // fetch local memory
  // int iter = 63;
  // uint64_t addr;
  // uint32_t lkey;
  // while(iter--){
  //   heap->fetch_mem_block_remote(addr, lkey);
  // }
  // std::cout << "addr: " << std::hex << addr << " rkey: " << lkey << std::endl;
  while (getchar()) {
    heap->print_alloc_info();
  }

  // }
  getchar();
//   ProfilerStop();
  heap->stop();
  delete heap;
  return 0;
}