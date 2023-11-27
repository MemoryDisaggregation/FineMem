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
#include "computing_node.h"
#include "rdma_conn.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <string>
#include <thread>

namespace mralloc {

const uint64_t BLOCK_SIZE = 1024*1024*4;

void * run_cache_filler(void* arg) {
  ComputingNode *heap = (ComputingNode*)arg;
  heap->cache_filler();
  return NULL;
} 

void * run_pre_fetcher(void* arg) {
  ComputingNode *heap = (ComputingNode*)arg;
  heap->pre_fetcher();
  return NULL;
} 

void* run_recycler(void* arg) {
    ComputingNode *heap = (ComputingNode*)arg;
    heap->recycler();
    return NULL;
}

/**
  * @description: start local memory service
  * @param {string} addr   the address string of RemoteHeap to connect
  * @param {string} port   the port of RemoteHeap to connect
  * @return {bool} true for success
  */
bool ComputingNode::start(const std::string addr, const std::string port){
    use_global_rkey_ = true;
    m_rdma_conn_ = new ConnectionManager();
    if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_->init(addr, port, 1, 1)) return false;
    // init free queue manager, using REMOTE_BLOCKSIZE as init size
    set_global_rkey(m_rdma_conn_->get_global_rkey());
    // if(one_side_enabled_) {
    //   header_list = (block_header*)malloc(m_one_side_info_.m_block_num*sizeof(block_header));
    //   rkey_list = (uint32_t*)malloc(m_one_side_info_.m_block_num*sizeof(uint32_t));
    //   update_mem_metadata();
    //   update_rkey_metadata();
    // }
    running = 1;
    if(heap_enabled_ && one_side_enabled_) {

        bool ret;
        m_one_side_info_ = m_rdma_conn_->get_one_side_info();
        block_size_ = m_one_side_info_.block_size_;
        block_num_ = m_one_side_info_.block_num_;
        region_size_ = block_size_ * block_per_region;
        region_num_ = block_num_ / block_per_region;
        section_size_ = region_size_ * region_per_section;
        section_num_ = region_num_ / region_per_section;

        section_header_ = m_one_side_info_.section_header_;
        fast_region_ = (uint64_t)((section_e*)section_header_ + section_num_);
        region_header_ = (uint64_t)((fast_class*)fast_region_ + block_class_num*section_num_);
        block_rkey_ = (uint64_t)((region_e*)region_header_ + region_num_);
        class_block_rkey_ = (uint64_t)((uint32_t*)block_rkey_ + block_num_);
        heap_start_ = m_one_side_info_.heap_start_;

        ret = new_cache_section(0);
        ret &= new_cache_region(0);
        cache_watermark_high = 0.9;
        cache_watermark_low = 0.1;
        cache_upper_bound = 512;
        ret &= fill_cache_block(0);
        for(int i = 1; i < block_class_num; i++) {
            ret &= new_cache_region(i);
            class_cache_upper_bound[i] = 1;
            ret &= fill_cache_block(i);
        }
        if(!ret) {
            printf("init cache failed\n");
            return false;
        }
        pthread_create(&pre_fetch_thread_, NULL, run_pre_fetcher, this);   
    } else if(one_side_enabled_) {
        new_cache_section(0);
        new_cache_region(0);
    } else if(heap_enabled_) {
        printf("RPC not implemented now\n");
    }
    // init cpu cache, insert a block for each cpu cache ring buffer
    if(cpu_cache_enabled_) {
        uint64_t remote_addr; uint32_t remote_rkey;
        cpu_cache_ = new cpu_cache(BLOCK_SIZE);
        for(int i = 0; i < nprocs; i++){
            // TODO: here we just fill 10 blocks, an automated or valified number should be tested
            for(int j = 0; j<1; j++) {
                fetch_mem_block(remote_addr, remote_rkey);
                assert(remote_addr!=0);
                cpu_cache_->add_cache(i, remote_addr, remote_rkey);
                printf("init @%d of %d, addr:%lx rkey:%u\n", i, j, remote_addr, remote_rkey);
            }
        }
        pthread_t running_thread;
        pthread_create(&cache_fill_thread_, NULL, run_cache_filler, this);
        pthread_create(&recycle_thread_, NULL, run_recycler, this);   
        
    }
    return true;
}

// a infinite loop worker
void ComputingNode::pre_fetcher() {
    uint64_t update_time = time_stamp_;
    uint32_t length;
    while(running) {
        if(update_time != time_stamp_) {
            update_time = time_stamp_;
            length = ring_cache->get_length();
            // if(length < cache_upper_bound *0.3)
            //     fill_cache_block(0);
            // // printf("I'll do update\n");
            // if(length < cache_upper_bound * cache_watermark_low) {
            //     cache_upper_bound += 1;
            //     fill_cache_block(0);
            // } else if(length < cache_upper_bound && length > cache_upper_bound * cache_watermark_high) {
            //     cache_upper_bound -= 1;
            //     fill_cache_block(0);
            // } else if(length != cache_upper_bound ) {
            //     fill_cache_block(0);
            // }
        }
    }
}

void ComputingNode::recycler() {
    uint64_t addr;
    while(running) {
        for(int i = 0; i < nprocs; i++){
            while(cpu_cache_->fetch_free_cache(i, addr)) {
                free_mem_block(addr);
                printf("add free cache addr:%lx, current:%u\n", addr, ring_cache->get_length());
            }
        }
    }
}

// a infinite loop worker
void ComputingNode::cache_filler() {
  // scan the cpu cache and refill them
    time_stamp_ = 0; uint64_t update = 0;
    uint64_t cpu_cache_watermark[nprocs];
    uint64_t cpu_class_watermark[class_num];
    for(int i=0; i<nprocs; i++) {
        cpu_cache_watermark[i] = 1;
    }
    for(int i=0; i<class_num; i++) {
        cpu_class_watermark[i] = 1;
    }
    uint64_t init_addr_ = -1; uint32_t init_rkey_ = -1;
    while(running) {
        update = 0;
        for(int i = 0; i < nprocs; i++){
            int free_ = cpu_cache_->get_length(i);
            // empty    --> fill and +1
            // 1 left   --> fill
            // > 1 left --> fill and -1
            if(free_ == 0){
                if (cpu_cache_watermark[i] < 8)
                    cpu_cache_watermark[i] += 1;
                for( int j = 0; j < cpu_cache_watermark[i]; j++){
                    if(!fetch_mem_block(init_addr_, init_rkey_)){
                        printf("fetch local cache failed!\n");
                    }
                    cpu_cache_->add_cache(i, init_addr_, init_rkey_);
                }
                update += cpu_cache_watermark[i];
                // printf("success add cache @ %d, %lx - %u\n", i, init_addr_, init_rkey_);
            }
            else if(free_ < cpu_cache_watermark[i] && free_ > 1) {
                if(cpu_cache_watermark[1] > 1)
                    cpu_cache_watermark[i] -= 1;
                for( int j = 0; j < cpu_cache_watermark[i] - free_; j++){
                    if(!fetch_mem_block(init_addr_, init_rkey_)){
                        printf("fetch local cache failed!\n");
                    }
                    cpu_cache_->add_cache(i, init_addr_, init_rkey_);
                }
                update += cpu_cache_watermark[i] - free_;
            } else if(free_ == 1) {
                for( int j = 0; j < cpu_cache_watermark[i] - free_; j++){
                    if(!fetch_mem_block(init_addr_, init_rkey_)){
                        printf("fetch local cache failed!\n");
                    }
                    cpu_cache_->add_cache(i, init_addr_, init_rkey_);
                }
                update += cpu_cache_watermark[i] - free_;
            } 
        }
        // TODO: the two phase are lineral, time cost may be large?
        for(int i = 1; i < class_num; i++){ 
            int free_ = cpu_cache_->get_class_length(i);
            if(free_ == 0){
               if ( cpu_class_watermark[i] < 8)
                    cpu_class_watermark[i] += 1;
                for( int j = 0; j < cpu_class_watermark[i]; j++){
                    if(!fetch_mem_class_block(i, init_addr_, init_rkey_)){
                        printf("fetch local cache failed!\n");
                    }
                    cpu_cache_->add_class_cache(i, init_addr_, init_rkey_);
                }
                update += cpu_class_watermark[i];
                // printf("success add cache @ %d, %lx - %u\n", i, init_addr_, init_rkey_);
            }
            else if(free_ < cpu_class_watermark[i] && free_ > 1) {
                if(cpu_class_watermark[1] > 1)
                    cpu_class_watermark[i] -= 1;
                for( int j = 0; j < cpu_class_watermark[i] - free_; j++){
                    if(!fetch_mem_class_block(i, init_addr_, init_rkey_)){
                        printf("fetch local cache failed!\n");
                    }
                    cpu_cache_->add_class_cache(i, init_addr_, init_rkey_);
                }
                update += cpu_class_watermark[i] - free_;
            } else if(free_ == 1) {
                for( int j = 0; j < cpu_class_watermark[i] - free_; j++){
                    if(!fetch_mem_class_block(i, init_addr_, init_rkey_)){
                        printf("fetch local cache failed!\n");
                    }
                    cpu_cache_->add_class_cache(i, init_addr_, init_rkey_);
                }
                update += cpu_class_watermark[i] - free_;
            }
        }
        if(update) {
            time_stamp_ += 1;
            printf("filling total %lu\n",update);
        }
        // printf("I'm running!\n");
    }
}

bool ComputingNode::new_cache_section(uint32_t block_class){
    alloc_advise advise = (block_class == 0?alloc_no_class:alloc_class);
    if(!m_rdma_conn_->find_section(current_section_, current_section_index_, advise) ) {
        printf("cannot find avaliable section\n");
        return false;
    }
    return true;
}

bool ComputingNode::new_cache_region(uint32_t block_class) {
    if(block_class == 0){
        // exclusive, and fetch rkey must
        while(!m_rdma_conn_->fetch_region(current_section_, current_section_index_, block_class, false, current_region_.region) ) {
            if(!new_cache_section(block_class)){
                return false;
            }
        }
        m_rdma_conn_->fetch_exclusive_region_rkey(current_region_.region, current_region_.rkey);
    } else {
        while(!m_rdma_conn_->fetch_region(current_section_, current_section_index_, block_class, true, current_class_region_[block_class]) ) {
            if(!new_cache_section(block_class)) {
                return false;
            }
        }
    }
    return true;
}

bool ComputingNode::fill_cache_block(uint32_t block_class){
    if(block_class == 0){
        uint32_t length =  ring_cache->get_length();
        for(int i = 0; i< cache_upper_bound - length; i++){
            if(current_region_.region.base_map_ != bitmap32_filled) {
                int index = find_free_index_from_bitmap32_tail(current_region_.region.base_map_);
                current_region_.region.base_map_ |= 1<<index;
                rdma_addr addr(get_region_block_addr(current_region_.region, index), current_region_.rkey[index]);
                printf("fill cache:%lx\n", addr.addr);
                ring_cache->add_cache(addr);
            } else {
                printf("no backup region, just fetch new region\n");
                exclusive_region_[current_region_.region.offset_] = current_region_;
                if(new_cache_region(block_class)) {
                    return fill_cache_block(block_class);
                } else return false;
            }
        }
    } else {
        for(int i = 0; i<class_cache_upper_bound[block_class]; i++){
            rdma_addr addr;
            while(!m_rdma_conn_->fetch_region_class_block(current_class_region_[block_class], block_class, addr.addr, 
                addr.rkey, false)) {
                // fetch new region
                new_cache_region(block_class);
            }
            ring_class_cache[block_class]->add_cache(addr);
        }
    }
    return true;
}

bool ComputingNode::fetch_mem_block(uint64_t &addr, uint32_t &rkey){
    rdma_addr result;
    bool ret = ring_cache->force_fetch_cache(result);
    addr = result.addr; rkey = result.rkey;
    return ret;
}

bool ComputingNode::fetch_mem_class_block(uint16_t block_class, uint64_t &addr, uint32_t &rkey){
    rdma_addr result;
    bool ret = ring_class_cache[block_class]->force_fetch_cache(result);
    addr = result.addr; rkey = result.rkey;
    return ret;
}

bool ComputingNode::free_mem_block(uint64_t addr){
    uint64_t region_offset = (addr - heap_start_) / region_size_;
    uint64_t region_block_offset = (addr - heap_start_) % region_size_ / block_size_;
    region_with_rkey* region;
    if(region_offset == current_region_.region.offset_) {
        region = &current_region_;    
    }
    else {
        if( exclusive_region_.find(region_offset) == exclusive_region_.end() ){
            printf("Not an exclusive block! And the shared block free is not completed\n");
            return false;
        } 
        region = &exclusive_region_[region_offset];
    }
    // m_rdma_conn_->remote_memzero(addr, (region->region.block_class_+1)*block_size_);
    if(!m_rdma_conn_->remote_rebind(addr, region->region.block_class_, region->rkey[region_block_offset])){
        // region->region.base_map_ &= ~(uint64_t)(1<<region_block_offset);
        rdma_addr value(addr, region->rkey[region_block_offset]);
        ring_cache->add_cache(value);
        // if(free_bit_in_bitmap32(region->region.base_map_) > block_per_region/2) {
        //     printf("time to reuse this region!\n");
        // }
        return true;
    }
    return false;
}

bool ComputingNode::fetch_mem_block_nocached(uint64_t &addr, uint32_t &rkey){
    while(!m_rdma_conn_->fetch_region_block(current_region_.region, addr,rkey, false)) {
        // printf("fetch new region\n");
        new_cache_region(0);
    }
    // printf("fill cache:%lx\n", ring_cache[writer].addr);
    if(use_global_rkey_) rkey = get_global_rkey();
    return true;
}

// bool ComputingNode::fetch_mem_block_one_sided(uint64_t &addr, uint32_t &rkey) {
//   return m_rdma_conn_->remote_fetch_block_one_sided(addr, rkey);
// }
 
// bool ComputingNode::update_rkey_metadata() {
//   uint64_t rkey_size = m_one_side_info_.m_block_num * sizeof(uint32_t);
//   m_rdma_conn_->remote_read(rkey_list, rkey_size, m_one_side_info_.m_rkey_addr_, get_global_rkey());
//   return true;
// }

// bool ComputingNode::update_mem_metadata() {
//   uint64_t metadata_size = m_one_side_info_.m_block_num * sizeof(block_header);
//   m_rdma_conn_->remote_read(header_list, metadata_size, m_one_side_info_.m_header_addr_, get_global_rkey());
//   return true;
// }

// bool ComputingNode::remote_fetch_block_one_sided(uint64_t &addr, uint32_t &rkey) {
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

void ComputingNode::fetch_cache(uint8_t nproc, uint64_t &addr, uint32_t &rkey) {
  cpu_cache_->fetch_cache(nproc, addr, rkey);
  return;
}

/**
  * @description: stop local memory service
  * @return {void}
  */
void ComputingNode::stop(){
  running = 0;
  if(cpu_cache_enabled_) cpu_cache_->free_cache();
//   free_queue_manager->print_state();
    // TODO
};

/**
  * @description: get memory alive state
  * @return {bool}  true for alive
  */
bool ComputingNode::alive() { return true; }

// // fetch memory in local side
// bool ComputingNode::fetch_mem_block(uint64_t &addr, uint32_t &rkey){
//   // free_queue_manager->fetch_block(addr, rkey);
//   if(!free_queue_manager->fetch_block(addr, rkey)) {
//     for(int i=0;i<RUNTIME_WATERMARK;i++){
//       uint64_t fetch_addr_; uint32_t fetch_rkey_;
//       fetch_mem_block_remote(fetch_addr_, fetch_rkey_);
//       if (!free_queue_manager->fill_block(fetch_addr_, BLOCK_SIZE, fetch_rkey_)){
//         printf("Remote fetch failed!\n");
//         return false;
//       }
//     }
//     free_queue_manager->fetch_block(addr, rkey);
//     // printf("success get remote, %lu\n", addr);
//   }
//   return true;
// }

/**
 * @description: get block memory chunk address from remote heap
 * @param {uint64_t} &addr
 * @param {uint32_t} &rkey
 * @return {bool} 
 */  
bool ComputingNode::fetch_mem_block_remote(uint64_t &addr, uint32_t &rkey) {
  // uint32_t rkey;

    if (m_rdma_conn_->remote_fetch_block(addr, rkey)) return false;
    return true;
}
 
bool ComputingNode::mr_bind_remote(uint64_t size, uint64_t addr, uint32_t rkey, uint32_t &newkey) {
  if (m_rdma_conn_->remote_mw(addr, rkey, size, newkey)) return false;
  return true;
}


}