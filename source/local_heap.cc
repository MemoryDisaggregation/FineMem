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

#include <bits/stdint-uintn.h>
#include <pthread.h>
#include <sys/types.h>
#include "memory_heap.h"
#include <cstdio>
#include <string>
#include <thread>

namespace mralloc {

const uint64_t REMOTE_BLOCKSIZE = 1024*1024*64;
const uint64_t LOCAL_BLOCKSIZE = 1024*1024*2;

void * run_heap(void* arg) {
  MemHeap *heap = (MemHeap*)arg;
  heap->run();
  return NULL;
}


/**
  * @description: start local memory service
  * @param {string} addr   the address string of RemoteHeap to connect
  * @param {string} port   the port of RemoteHeap to connect
  * @return {bool} true for success
  */
bool LocalHeap::start(const std::string addr, const std::string port){
    uint64_t init_addr_ = 0;
    m_rdma_conn_ = new ConnectionManager();
    if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_->init(addr, port, 4, 20)) return false;
    // init free queue manager, using REMOTE_BLOCKSIZE as init size
    free_queue_manager = new FreeQueueManager(LOCAL_BLOCKSIZE);
    fetch_mem_fast_remote(init_addr_);
    free_queue_manager->init(init_addr_, REMOTE_BLOCKSIZE);
    // init cpu cache, insert a block for each cpu cache ring buffer
    cpu_cache_ = new cpu_cache(LOCAL_BLOCKSIZE);
    for(int i = 0; i < nprocs; i++){
      if(cpu_cache_->is_empty(i)){
        // TODO: here we just fill 10 blocks, an automated or valified number should be tested
        for( int j = 0; j < 10; j++){
          fetch_mem_fast(init_addr_);
          printf("init @%d, addr:%ld\n", i, init_addr_);
          cpu_cache_->add_cache(i, init_addr_);
        }
      }
    }
    // create a thread to run()
    running = 1;
    pthread_t running_thread;
    pthread_create(&running_thread, NULL, run_heap, this);
    return true;
}

// who will call run()? the host may run with a readline to exit, and create a new thread to run.
void LocalHeap::run() {
  // scan the cpu cache and refill them
  uint64_t init_addr_ = 0;
  while(running) {
    for(int i = 0; i < nprocs; i++){
      // if empty, fill it with 10 blocks
      // TODO: a automated filler, will choose how much blocks to fill
      if(cpu_cache_->is_empty(i)){
        // TODO: an iteration to call times of fetch blocks is somehow too ugly
        for( int j = 0; j < 10; j++){
          fetch_mem_fast(init_addr_);
          cpu_cache_->add_cache(i, init_addr_);
        }
      }
    }
  }
}

uint64_t LocalHeap::fetch_cache(uint8_t nproc){
  return cpu_cache_->fetch_cache(nproc);
}

/**
  * @description: stop local memory service
  * @return {void}
  */
void LocalHeap::stop(){
  running = 0;
  cpu_cache_->free_cache();
    // TODO
};

/**
  * @description: get memory alive state
  * @return {bool}  true for alive
  */
bool LocalHeap::alive() { return true; }

// fetch memory in local side
bool LocalHeap::fetch_mem_fast(uint64_t &addr){
  addr = free_queue_manager->fetch_fast();
  if(addr == 0) return false;
  return true;
}

/**
 * @description: get 2MB memory chunk address from remote heap
 * @param {uint64_t} &addr
 * @param {uint32_t} &rkey
 * @return {bool} 
 */  
bool LocalHeap::fetch_mem_fast_remote(uint64_t &addr) {
  uint32_t rkey;
  if (m_rdma_conn_->remote_fetch_fast_block(addr, rkey)) return false;
  return true;
}

}