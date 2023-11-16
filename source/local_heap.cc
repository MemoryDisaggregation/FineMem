/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:08:03
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-14 17:47:06
 * @FilePath: /rmalloc_newbase/source/local_heap.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/stdint-uintn.h>
#include <pthread.h>
#include <sys/types.h>
#include "free_block_manager.h"
#include "memory_heap.h"
#include "rdma_conn.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <thread>

namespace mralloc {

const uint64_t BLOCK_SIZE = 1024*1024*32;
const uint64_t INIT_WATERMARK = 80;
const uint64_t RUNTIME_WATERMARK = 20;

void * run_cache_filler(void* arg) {
  LocalHeap *heap = (LocalHeap*)arg;
  heap->cache_filler();
  return NULL;
} 


/**
  * @description: start local memory service
  * @param {string} addr   the address string of RemoteHeap to connect
  * @param {string} port   the port of RemoteHeap to connect
  * @return {bool} true for success
  */
bool LocalHeap::start(const std::string addr, const std::string port){
    m_rdma_conn_ = new ConnectionManager();
    if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_->init(addr, port, 40, 40)) return false;
    // init free queue manager, using REMOTE_BLOCKSIZE as init size
    set_global_rkey(m_rdma_conn_->get_global_rkey());
    // if(one_side_enabled_) {
    //   m_one_side_info_ = m_rdma_conn_->get_one_side_info();
    //   header_list = (block_header*)malloc(m_one_side_info_.m_block_num*sizeof(block_header));
    //   rkey_list = (uint32_t*)malloc(m_one_side_info_.m_block_num*sizeof(uint32_t));
    //   update_mem_metadata();
    //   update_rkey_metadata();
    // }
    if(heap_enabled_) {
      uint64_t remote_addr; uint32_t remote_rkey;
      free_queue_manager = new FreeQueueManager(BLOCK_SIZE);
      free_queue_manager->init(0, 0, 0);
      for(int i = 0;i<INIT_WATERMARK; i++) {
        if(!fetch_mem_block_remote(remote_addr, remote_rkey)){
          printf("init fetch failed!\n");
        }
        free_queue_manager->fill_block(remote_addr, BLOCK_SIZE, remote_rkey);
      }
      // set_global_rkey(init_rkey_);      
    }
    // init cpu cache, insert a block for each cpu cache ring buffer
    if(cpu_cache_enabled_) {
      uint64_t remote_addr; uint32_t remote_rkey;
      cpu_cache_ = new cpu_cache(BLOCK_SIZE);
      for(int i = 0; i < nprocs; i++){
        // if(cpu_cache_->is_empty(i)){
          // TODO: here we just fill 10 blocks, an automated or valified number should be tested
          fetch_mem_block(remote_addr, remote_rkey);
          assert(remote_addr!=0);
          cpu_cache_->add_cache(i, remote_addr, remote_rkey);
          printf("init @%d, addr:%lx rkey:%u\n", i, remote_addr, remote_rkey);
        // }
      }
      running = 1;
      pthread_t running_thread;
      heap_worker_id_ = 0;
      heap_worker_num_ = 1;
      for(int i =0;i< heap_worker_num_;i++)
        pthread_create(&running_thread, NULL, run_cache_filler, this);
    }
    return true;
}

// who will call run()? the host may run with a readline to exit, and create a new thread to run.
void LocalHeap::cache_filler() {
  // scan the cpu cache and refill them
  uint64_t cpu_cache_history[nprocs];
  for(int i=0; i<nprocs; i++) {
    cpu_cache_history[i] = 1;
  }
  uint64_t init_addr_ = 0; uint32_t init_rkey_;
  uint8_t id = heap_worker_id_++;
  while(running) {
    for(int i = id; i < nprocs; i+=heap_worker_num_){
      // if empty, fill it with 10 blocks
      // TODO: a automated filler, will choose how much blocks to fill
      int free_ = cpu_cache_->get_length(i);
      // if(cpu_cache_->is_empty(i)){
      if(free_ != cpu_cache_history[i] && free_ < 1){
        // TODO: an iteration to call times of fetch blocks is somehow too ugly
        // int free_ = cpu_cache_->get_length(i);
        cpu_cache_history[i] = 1;
        for( int j = 0; j < 1; j++){
          fetch_mem_block(init_addr_, init_rkey_);
          cpu_cache_->add_cache(i, init_addr_, init_rkey_);
        }
        printf("success add cache @ %d, %lx - %u\n", i, init_addr_, init_rkey_);
      }
    }
    // printf("I'm running!\n");
  }
}

bool LocalHeap::fetch_mem_block_one_sided(uint64_t &addr, uint32_t &rkey) {
  return m_rdma_conn_->remote_fetch_block_one_sided(addr, rkey);
}
 
// bool LocalHeap::update_rkey_metadata() {
//   uint64_t rkey_size = m_one_side_info_.m_block_num * sizeof(uint32_t);
//   m_rdma_conn_->remote_read(rkey_list, rkey_size, m_one_side_info_.m_rkey_addr_, get_global_rkey());
//   return true;
// }

// bool LocalHeap::update_mem_metadata() {
//   uint64_t metadata_size = m_one_side_info_.m_block_num * sizeof(block_header);
//   m_rdma_conn_->remote_read(header_list, metadata_size, m_one_side_info_.m_header_addr_, get_global_rkey());
//   return true;
// }

// bool LocalHeap::remote_fetch_block_one_sided(uint64_t &addr, uint32_t &rkey) {
//   uint64_t large_block_num = m_one_side_info_.m_block_num;
//   uint64_t block_size = m_one_side_info_.m_block_size;
//   uint64_t base_size = m_one_side_info_.m_base_size;
//   for(int i = 0; i< large_block_num; i++){
//     uint64_t index = (i+last_alloc_)%large_block_num;
//     if(header_list[index].max_length == block_size/base_size && (header_list[index].flag & (uint64_t)1) == 1){
//       block_header update_header = header_list[index];
//       update_header.flag &= ~((uint64_t)1);
//       uint64_t swap_value = *(uint64_t*)(&update_header); 
//       uint64_t cmp_value = *(uint64_t*)(&header_list[index]);
//       uint64_t result = m_rdma_conn_->remote_CAS(swap_value, cmp_value, 
//                                                   m_one_side_info_.m_header_addr_ + index * sizeof(block_header), 
//                                                   get_global_rkey());
//       if (result != cmp_value) {
//         update_mem_metadata();
//       } else {
//         last_alloc_ = index;
//         addr = m_one_side_info_.m_block_addr_ + index * m_one_side_info_.m_block_size;
//         rkey = rkey_list[index];
//         return true;
//       }
//     }
//   }
//   return false;
// }

void LocalHeap::fetch_cache(uint8_t nproc, uint64_t &addr, uint32_t &rkey) {
  cpu_cache_->fetch_cache(nproc, addr, rkey);
  return;
}

/**
  * @description: stop local memory service
  * @return {void}
  */
void LocalHeap::stop(){
  running = 0;
  cpu_cache_->free_cache();
  free_queue_manager->print_state();
    // TODO
};

/**
  * @description: get memory alive state
  * @return {bool}  true for alive
  */
bool LocalHeap::alive() { return true; }

// fetch memory in local side
bool LocalHeap::fetch_mem_block(uint64_t &addr, uint32_t &rkey){
  // free_queue_manager->fetch_block(addr, rkey);
  if(!free_queue_manager->fetch_block(addr, rkey)) {
    for(int i=0;i<RUNTIME_WATERMARK;i++){
      uint64_t fetch_addr_; uint32_t fetch_rkey_;
      fetch_mem_block_remote(fetch_addr_, fetch_rkey_);
      if (!free_queue_manager->fill_block(fetch_addr_, BLOCK_SIZE, fetch_rkey_)){
        printf("Remote fetch failed!\n");
        return false;
      }
    }
    free_queue_manager->fetch_block(addr, rkey);
    // printf("success get remote, %lu\n", addr);
  }
  return true;
}

/**
 * @description: get block memory chunk address from remote heap
 * @param {uint64_t} &addr
 * @param {uint32_t} &rkey
 * @return {bool} 
 */  
bool LocalHeap::fetch_mem_block_remote(uint64_t &addr, uint32_t &rkey) {
  // uint32_t rkey;
  if(one_side_enabled_){
    if (m_rdma_conn_->remote_fetch_block_one_sided(addr, rkey)) return false;
  } else {
    if (m_rdma_conn_->remote_fetch_block(addr, rkey)) return false;
  }
  return true;
}
 
bool LocalHeap::mr_bind_remote(uint64_t size, uint64_t addr, uint32_t rkey, uint32_t &newkey) {
  if (m_rdma_conn_->remote_mw(addr, rkey, size, newkey)) return false;
  return true;
}


}