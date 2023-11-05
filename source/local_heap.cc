/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:08:03
 * @LastEditors: blahaj wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-05 11:16:52
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

const uint64_t REMOTE_BLOCKSIZE = 1024*1024*32;
const uint64_t LOCAL_BLOCKSIZE = 1024*1024*32;

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
    uint32_t init_rkey_ = 0;
    m_rdma_conn_ = new ConnectionManager();
    if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_->init(addr, port, 1, 1)) return false;
    // init free queue manager, using REMOTE_BLOCKSIZE as init size
    set_global_rkey(m_rdma_conn_->get_global_rkey());
    if(one_side_enabled_) {
      m_one_side_info_ = m_rdma_conn_->get_one_side_info();
      header_list = (block_header*)malloc(m_one_side_info_.m_block_num*sizeof(block_header));
      rkey_list = (uint32_t*)malloc(m_one_side_info_.m_block_num*sizeof(uint32_t));
      update_mem_metadata();
      update_rkey_metadata();
    }
    if(heap_enabled_) {
      free_queue_manager = new FreeQueueManager(LOCAL_BLOCKSIZE);
      if(!fetch_mem_fast_remote(init_addr_, init_rkey_)){
        printf("init fetch failed!\n");
      }
      set_global_rkey(init_rkey_);
      free_queue_manager->init(init_addr_, REMOTE_BLOCKSIZE, init_rkey_);
    }
    // init cpu cache, insert a block for each cpu cache ring buffer
    if(cpu_cache_enabled_) {
      cpu_cache_ = new cpu_cache(LOCAL_BLOCKSIZE);
      for(int i = 0; i < nprocs; i++){
        if(cpu_cache_->is_empty(i)){
          // TODO: here we just fill 10 blocks, an automated or valified number should be tested
          for( int j = 0; j < 1; j++){
            fetch_mem_fast(init_addr_, init_rkey_);
            assert(init_addr_!=0);
            cpu_cache_->add_cache(i, init_addr_, init_rkey_);
          }
          // printf("init @%d, addr:%lx\n", i, init_addr_);
        }
      }
      running = 1;
      pthread_t running_thread;
      heap_worker_id_ = 0;
      heap_worker_num_ = 4;
      for(int i =0;i< heap_worker_num_;i++)
        pthread_create(&running_thread, NULL, run_heap, this);
    }
    return true;
}

// who will call run()? the host may run with a readline to exit, and create a new thread to run.
void LocalHeap::run() {
  // scan the cpu cache and refill them
  uint64_t cpu_cache_history[nprocs] = {1};
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
        cpu_cache_history[i] = free_ + 8;
        for( int j = 0; j < 1; j++){
          fetch_mem_fast(init_addr_, init_rkey_);
          cpu_cache_->add_cache(i, init_addr_, init_rkey_);
        }
        // printf("success add cache @ %d, %lu\n", i, init_addr_);
      }
    }
    // printf("I'm running!\n");
  }
}

bool LocalHeap::update_rkey_metadata() {
  uint64_t rkey_size = m_one_side_info_.m_block_num * sizeof(uint32_t);
  m_rdma_conn_->remote_read(rkey_list, rkey_size, m_one_side_info_.m_rkey_addr_, get_global_rkey());
  return true;
}

bool LocalHeap::update_mem_metadata() {
  uint64_t metadata_size = m_one_side_info_.m_block_num * sizeof(block_header);
  m_rdma_conn_->remote_read(header_list, metadata_size, m_one_side_info_.m_header_addr_, get_global_rkey());
  return true;
}

bool LocalHeap::fetch_mem_one_sided(uint64_t &addr, uint32_t &rkey) {
  uint64_t block_num = m_one_side_info_.m_block_num;
  uint64_t fast_size = m_one_side_info_.m_fast_size;
  uint64_t base_size = m_one_side_info_.m_base_size;
  for(int i = 0; i< block_num; i++){
    uint64_t index = (i+last_alloc_)%block_num;
    if(header_list[index].max_length == fast_size/base_size && (header_list[index].flag & (uint64_t)1) == 1){
      block_header update_header = header_list[index];
      update_header.flag &= ~((uint64_t)1);
      uint64_t swap_value = *(uint64_t*)(&update_header); 
      uint64_t cmp_value = *(uint64_t*)(&header_list[index]);
      uint64_t result = m_rdma_conn_->remote_CAS(swap_value, cmp_value, 
                                                  m_one_side_info_.m_header_addr_ + index * sizeof(block_header), 
                                                  get_global_rkey());
      if (result != cmp_value) {
        update_mem_metadata();
      } else {
        last_alloc_ = index;
        addr = m_one_side_info_.m_block_addr_ + index * m_one_side_info_.m_fast_size;
        rkey = rkey_list[index];
        return true;
      }
    }
  }
  return false;
}

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
bool LocalHeap::fetch_mem_fast(uint64_t &addr, uint32_t &rkey){
  // free_queue_manager->fetch_fast(addr, rkey);
  if(!free_queue_manager->fetch_fast(addr, rkey)) {
    uint64_t fetch_addr_; uint32_t fetch_rkey_;
    fetch_mem_fast_remote(fetch_addr_, fetch_rkey_);
    if (!free_queue_manager->return_back(fetch_addr_, REMOTE_BLOCKSIZE, fetch_rkey_)){
      printf("Remote fetch failed!\n");
      return false;
    }
    free_queue_manager->fetch_fast(addr, rkey);
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
bool LocalHeap::fetch_mem_fast_remote(uint64_t &addr, uint32_t &rkey) {
  // uint32_t rkey;
  if (m_rdma_conn_->remote_fetch_fast_block(addr, rkey)) return false;
  return true;
}

bool LocalHeap::mr_bind_remote(uint64_t size, uint64_t addr, uint32_t rkey, uint32_t &newkey) {
  if (m_rdma_conn_->remote_mw(addr, rkey, size, newkey)) return false;
  return true;
}


}