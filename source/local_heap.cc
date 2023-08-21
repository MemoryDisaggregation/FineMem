/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:08:03
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-08-14 16:41:55
 * @FilePath: /rmalloc_newbase/source/local_heap.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include "memory_heap.h"
#include <cstdio>
#include <string>

namespace mralloc {

/**
  * @description: start local memory service
  * @param {string} addr   the address string of RemoteHeap to connect
  * @param {string} port   the port of RemoteHeap to connect
  * @return {bool} true for success
  */
bool LocalHeap::start(const std::string addr, const std::string port){
    m_rdma_conn_ = new ConnectionManager();
    if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_->init(addr, port, 4, 20)) return false;
    return true;
}

/**
  * @description: stop local memory service
  * @return {void}
  */
void LocalHeap::stop(){
    // TODO
};

/**
  * @description: get memory alive state
  * @return {bool}  true for alive
  */
bool LocalHeap::alive() { return true; }

/**
 * @description: get 2MB memory chunk address from remote heap
 * @param {uint64_t} &addr
 * @param {uint32_t} &rkey
 * @return {bool} 
 */  
bool LocalHeap::fetch_mem_fast(uint64_t &addr) {
  uint32_t rkey;
  if (m_rdma_conn_->remote_fetch_fast_block(addr, rkey)) return false;
  return true;
}

}