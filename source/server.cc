/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:21:21
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-08-14 16:27:22
 * @FilePath: /rmalloc_newbase/source/server.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <string>
#include <iostream>
#include "memory_heap.h"

int main(int argc, char *argv[]) {
  if (argc < 3) {
    std::cout << "Usage: " << argv[0] << " <ip> <port>" << std::endl;
    return 0;
  }

  std::string ip = argv[1];
  std::string port = argv[2];

  mralloc::RemoteHeap *heap = new mralloc::RemoteHeap();
  heap->start(ip, port);

  // fetch local memory
  // int iter = 10;
  // uint64_t addr;
  // uint32_t lkey;
  // while(iter--){
  //   heap->fetch_mem_2MB(addr, lkey);
  //   std::cout << "addr: " << std::hex << addr << " rkey: " << lkey << std::endl;
  // }

  heap->stop();
  delete heap;
  return 0;
}