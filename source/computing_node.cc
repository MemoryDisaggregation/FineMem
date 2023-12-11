/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:08:03
 * @LastEditors: blahaj wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-27 22:39:05
 * @FilePath: /rmalloc_newbase/source/local_heap.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/stdint-uintn.h>
#include <pthread.h>
#include <sys/types.h>
#include "cpu_cache.h"
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
    sleep(1);
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
        exclusive_region_ = (region_with_rkey*)malloc(sizeof(region_with_rkey) * region_num_);

        ret = new_cache_section(0, alloc_empty);
        ret &= new_backup_section();
        ret &= new_cache_region(0);
        ret &= new_backup_region();
        cache_watermark_high = 0.9;
        cache_watermark_low = 0.1;
        cache_upper_bound = 1000;
        ret &= fill_cache_block(0);
        for(int i = 1; i < block_class_num; i++) {
            ret &= new_cache_region(i);
            class_cache_upper_bound[i] = 2;
            ret &= fill_cache_block(i);
        }
        // class_cache_upper_bound[7] = 256;
        // fill_cache_block(7);
        if(!ret) {
            printf("init cache failed\n");
            return false;
        }
    } else if(one_side_enabled_) {
        new_cache_section(0, alloc_empty);
        new_cache_region(0);
    } else if(heap_enabled_) {
        printf("RPC not implemented now\n");
    }
    // init cpu cache, insert a block for each cpu cache ring buffer
    if(cpu_cache_enabled_) {
        uint64_t remote_addr; uint32_t remote_rkey;
        cpu_cache_ = new cpu_cache(BLOCK_SIZE);
        for(int i = 0; i < nprocs; i++){
            fetch_mem_block(remote_addr, remote_rkey);
            assert(remote_addr!=0);
            cpu_cache_->add_cache(i, remote_addr, remote_rkey);
            printf("init @%d addr:%lx rkey:%u\n", i, remote_addr, remote_rkey);
        }
        for(int i = 1; i < class_num; i++){
            fetch_mem_class_block(i, remote_addr, remote_rkey);
            assert(remote_addr!=0);
            cpu_cache_->add_class_cache(i, remote_addr, remote_rkey);
            printf("init class %d addr:%lx rkey:%u\n", i, remote_addr, remote_rkey);
        }
        pthread_t running_thread;
        pthread_create(&cache_fill_thread_, NULL, run_cache_filler, this);
        pthread_create(&recycle_thread_, NULL, run_recycler, this);   
        
    }
    if(heap_enabled_ && one_side_enabled_) 
        pthread_create(&pre_fetch_thread_, NULL, run_pre_fetcher, this);   

    return true;
}

void ComputingNode::increase_class_watermark(int &upper_bound) {
    if(upper_bound < block_per_region) {
        upper_bound *= 2;
    } else {
        upper_bound += 1;
    }
}

void ComputingNode::increase_watermark(int &upper_bound) {
    if(upper_bound < block_per_region) {
        upper_bound *= 2;
    }
    else if(upper_bound >= 2*block_per_region) {
        upper_bound += block_per_region;
    } else {
        upper_bound += 8;
    }
}

void ComputingNode::decrease_watermark(int &upper_bound) {
    if(upper_bound > 2*block_per_region) {
        upper_bound -= block_per_region;
    } else if (upper_bound > block_per_region){
        upper_bound -= 1;
    }
}

// a infinite loop worker
void ComputingNode::pre_fetcher() {
    uint64_t update_time = time_stamp_;
    uint32_t length;
    bool block_breakdown = false, class_breakdown = false;
    cache_upper_bound = block_per_region;
    for(int i = 1; i < class_num; i++) {
        class_cache_upper_bound[i] = 1;
    }
    while(running) {
        if(ring_cache->get_length() == 0) {
            block_breakdown = true;
            increase_watermark(cache_upper_bound);
            fill_cache_block(0);
            // printf("watermark: %lu, free space: %u, total used mem:%luMiB\n", cache_upper_bound, ring_cache->get_length(), fill_counter*4);
        }
        for(int i = 1; i < class_num; i++) {
            if(ring_class_cache[i]->get_length() == 0) {
                class_breakdown = true;
                increase_class_watermark(class_cache_upper_bound[i]);
                fill_cache_block(i);
                // printf("class %d watermark: %lu, free space: %u, total used mem:%luMiB\n", i, class_cache_upper_bound[i], ring_class_cache[i]->get_length(), fill_counter*4);
            }
        }
        if(update_time != time_stamp_) {
            update_time = time_stamp_;
            // fill the block cache
            if(block_breakdown) {
                block_breakdown = false;
            } else {
                length = ring_cache->get_length();
                if(length <= cache_upper_bound * cache_watermark_low) {
                    increase_watermark(cache_upper_bound);
                    fill_cache_block(0);
                } else if(length < cache_upper_bound && length >= cache_upper_bound * cache_watermark_high) {
                    decrease_watermark(cache_upper_bound);
                    fill_cache_block(0);
                } else if(length < cache_upper_bound ) {
                    fill_cache_block(0);
                    // printf("fill: %lu ", cache_upper_bound - length);
                }
            }

            // fill the class block cache
            if(class_breakdown) {
                class_breakdown = false;
            } else {
                for(int i = 1; i < class_num; i++) {
                    length = ring_class_cache[i]->get_length();
                    if(length <= class_cache_upper_bound[i] * cache_watermark_low) {
                        increase_class_watermark(class_cache_upper_bound[i]);
                        fill_cache_block(i);
                    } else if(length < class_cache_upper_bound[i] && length >= class_cache_upper_bound[i] * cache_watermark_high) {
                        decrease_watermark(class_cache_upper_bound[i]) ;
                        fill_cache_block(i);
                    } else if(length < class_cache_upper_bound[i] ) {
                        fill_cache_block(i);
                        // printf("fill: %lu ", cache_upper_bound - length);
                    }
                    // printf("class %d watermark: %lu, free space: %u, total used mem:%luMiB\n", i, class_cache_upper_bound[i], ring_class_cache[i]->get_length(), fill_counter*4);

                }
            }
            // printf("watermark: %lu, free space: %u, total used mem:%luMiB\n", cache_upper_bound, ring_cache->get_length(), fill_counter*4);
        }
    }
}

void ComputingNode::recycler() {
    uint64_t addr, batch_addr[max_free_item], length;
    while(running) {
        for(int i = 0; i < nprocs; i++){
            while((length = cpu_cache_->fetch_free_cache(i, batch_addr)) != 0) {
                for(int j = 0; j < length; j++)
                    free_mem_block(batch_addr[j]);
                // printf("add free cache addr:%lx, current:%u\n", addr, ring_cache->get_length());
            }
        }
        for(int i = 0; i < class_num; i++){
            while(cpu_cache_->fetch_class_free_cache(i, addr)) {
                free_mem_block(addr);
                // printf("add free class %d cache addr:%lx, current:%u\n", i, addr, ring_class_cache[i]->get_length());
            }
        }
    }
}

void ComputingNode::print_cpu_cache() {
    for(int i = 0;i < nprocs; i++) {
        if(cpu_cache_watermark[i] != 1 || cpu_cache_->get_length(i) != 1)
            printf("%d:%d/%lu\t", i, cpu_cache_->get_length(i), cpu_cache_watermark[i]);
    }
    printf("\n");

}

// a infinite loop worker
void ComputingNode::cache_filler() {
  // scan the cpu cache and refill them
    time_stamp_ = 0; uint64_t update = 0;

    for(int i=0; i<nprocs; i++) {
        cpu_cache_watermark[i] = 1;
    }
    for(int i=0; i<class_num; i++) {
        cpu_class_watermark[i] = 1;
    }
    uint64_t init_addr_ = -1; uint32_t init_rkey_ = -1;
    mr_rdma_addr batch_addr[64];
    mr_rdma_addr class_batch_addr[64];
    while(running) {
        update = 0;
        for(int i = 0; i < nprocs; i++){
            int free_ = cpu_cache_->get_length(i);
            // empty    --> fill and +1
            // 1 left   --> fill
            // > 1 left --> fill and -1
            if(free_ == 0){
                if (cpu_cache_watermark[i] < 16)
                    cpu_cache_watermark[i] += 1;
                while(!ring_cache->try_fetch_batch(batch_addr, cpu_cache_watermark[i])){
                    time_stamp_ += 1;
                }
                cpu_cache_->add_batch(i, batch_addr, cpu_cache_watermark[i]);
                update += cpu_cache_watermark[i];
                // for(int j = 0; j< cpu_cache_watermark[i]; j++){
                //     printf("success add cache @ %d, %lx - %u\n", i, batch_addr[j].addr, batch_addr[j].rkey);
                // }
                // printf("success add cache @ %d, %lx - %u\n", i, init_addr_, init_rkey_);
            }
            else if(free_ < cpu_cache_watermark[i] && free_ > 1) {
                if(cpu_cache_watermark[1] > 1)
                    cpu_cache_watermark[i] -= 1;
                while(!ring_cache->try_fetch_batch(batch_addr, cpu_cache_watermark[i]-free_)){
                    time_stamp_ += 1;
                }
                cpu_cache_->add_batch(i, batch_addr, cpu_cache_watermark[i]-free_);
                // printf("success add cache @ %d, %lx - %u\n", i, init_addr_, init_rkey_);
                // for(int j = 0; j< cpu_cache_watermark[i]-free_; j++){
                //     printf("success add cache @ %d, %lx - %u\n", i, batch_addr[j].addr, batch_addr[j].rkey);
                // }
                update += cpu_cache_watermark[i] - free_;
            } else if(cpu_cache_watermark[i] > 1 && free_ == 1) {
                while(!ring_cache->try_fetch_batch(batch_addr, cpu_cache_watermark[i]-free_)){
                    time_stamp_ += 1;
                }
                cpu_cache_->add_batch(i, batch_addr, cpu_cache_watermark[i]-free_);
                // printf("success add cache @ %d, %lx - %u\n", i, init_addr_, init_rkey_);
                // for(int j = 0; j< cpu_cache_watermark[i]-free_; j++){
                //     printf("success add cache @ %d, %lx - %u\n", i, batch_addr[j].addr, batch_addr[j].rkey);
                // }
                update += cpu_cache_watermark[i] - free_;
            } 
        }
        // TODO: the two phase are lineral, time cost may be large?
        for(int i = 1; i < class_num; i++){ 
            int free_ = cpu_cache_->get_class_length(i);
            if(free_ == 0){
                // printf("class %d watermark:%d\n", i, cpu_class_watermark[i]);
               if ( cpu_class_watermark[i] < 16)
                    cpu_class_watermark[i] += 1;
                if(!ring_class_cache[i]->try_fetch_batch(class_batch_addr, cpu_class_watermark[i])){
                    for( int j = 0; j < cpu_class_watermark[i]; j++){
                        if(!fetch_mem_class_block(i, class_batch_addr[j].addr, class_batch_addr[j].rkey)){
                            printf("fetch local cache failed!\n");
                        }
                    }
                    time_stamp_ += 1;
                }
                cpu_cache_->add_class_batch(i, class_batch_addr, cpu_class_watermark[i]);
                update += cpu_class_watermark[i];
            }
            else if(free_ < cpu_class_watermark[i] && free_ > 1) {
                if(cpu_class_watermark[i] > 1)
                    cpu_class_watermark[i] -= 1;
                if(!ring_class_cache[i]->try_fetch_batch(class_batch_addr, cpu_class_watermark[i]-free_)){
                    for( int j = 0; j < cpu_class_watermark[i]-free_; j++){
                        if(!fetch_mem_class_block(i, class_batch_addr[j].addr, class_batch_addr[j].rkey)){
                            printf("fetch local cache failed!\n");
                        }
                    }
                    time_stamp_ += 1;
                }
                cpu_cache_->add_class_batch(i, class_batch_addr, cpu_class_watermark[i]-free_);
                update += cpu_class_watermark[i];

            } else if(cpu_class_watermark[i] > 1 && free_ == 1) {
                if(!ring_class_cache[i]->try_fetch_batch(class_batch_addr, cpu_class_watermark[i]-free_)){
                    for( int j = 0; j < cpu_class_watermark[i]-free_; j++){
                        if(!fetch_mem_class_block(i, class_batch_addr[j].addr, class_batch_addr[j].rkey)){
                            printf("fetch local cache failed!\n");
                        }
                    }
                    time_stamp_ += 1;
                }
                cpu_cache_->add_class_batch(i, class_batch_addr, cpu_class_watermark[i]-free_);
                update += cpu_class_watermark[i] - free_;
            }
        }
        if(update) {
            time_stamp_ += 1;
            // print_cpu_cache();
        }
        // printf("I'm running!\n");
    }
}

bool ComputingNode::new_cache_section(uint32_t block_class, alloc_advise advise){
    if(!m_rdma_conn_->find_section(current_section_, current_section_index_, advise) ) {
        printf("cannot find avaliable section\n");
        return false;
    }
    printf("get new cache section:%d\n", current_section_index_);

    return true;
}

bool ComputingNode::new_backup_section(){
    if(!m_rdma_conn_->find_section(backup_section_, backup_section_index_, alloc_no_class) ) {
        printf("cannot find avaliable section\n");
        return false;
    }
    printf("get new backup section:%d\n", backup_section_index_);
    return true;
}

bool ComputingNode::new_cache_region(uint32_t block_class) {
    if(block_class == 0){
        // exclusive, and fetch rkey must
        region_e new_region;
        while(!m_rdma_conn_->fetch_region(current_section_, current_section_index_, block_class, false, new_region) ) {
            printf("fetch_new section\n");
            if(!new_cache_section(block_class, alloc_empty)){
                return false;
            }
        }
        current_region_ = &exclusive_region_[new_region.offset_];
        current_region_->region = new_region;
        m_rdma_conn_->fetch_exclusive_region_rkey(current_region_->region, current_region_->rkey);
    } else {
        while(!m_rdma_conn_->fetch_region(current_section_, current_section_index_, block_class, true, current_class_region_[block_class]) ) {
            if(!new_cache_section(block_class, alloc_class)) {
                return false;
            }
        }
    }
    return true;
}

bool ComputingNode::new_backup_region() {
        // exclusive, and fetch rkey must
    while(!m_rdma_conn_->fetch_region(backup_section_, backup_section_index_, 0, true, backup_region_) ) {
        printf("fetch_new backup section\n");
        if(!new_backup_section()){
            return false;
        }
    }
    backup_counter += 1;
    // m_rdma_conn_->fetch_exclusive_region_rkey(current_region_.region, backup_region_.rkey);   
    return true;
}

bool ComputingNode::fill_cache_block(uint32_t block_class){
    if(block_class == 0){
        int length =  ring_cache->get_length();
        for(int i = 0; i< cache_upper_bound - length; i++){
            if(current_region_->region.base_map_ != bitmap32_filled) {
                int index = find_free_index_from_bitmap32_tail(current_region_->region.base_map_);
                current_region_->region.base_map_ |= 1<<index;
                mr_rdma_addr addr(get_region_block_addr(current_region_->region, index), current_region_->rkey[index]);
                // printf("exclusive addr: %p, rkey: %u\n", addr.addr, addr.rkey);
                // printf("fill cache:%lx\n", addr.addr);
                ring_cache->add_cache(addr);
                fill_counter += 1;
            } else {
                if(!free_region_.empty()) {
                    current_region_ = *free_region_.begin();
                    free_region_.erase(free_region_.begin());
                    // m_rdma_conn_->fetch_exclusive_region_rkey(current_region_->region, current_region_->rkey);
                    return fill_cache_block(block_class);
                }
                // printf("no backup region, just fetch new region\n");
                int block_num = cache_upper_bound - ring_cache->get_length(), get_num;
                mr_rdma_addr addr[block_per_region];
                while(block_num > 0){
                    while((get_num = m_rdma_conn_->fetch_region_batch(backup_region_, addr, block_num, false)) == 0){
                        if(backup_counter == backup_cycle) {
                            backup_counter = 0;
                            if(new_cache_region(block_class)) {
                                return fill_cache_block(block_class);
                                // continue;
                            } else return false;
                        }
                        new_backup_region();
                    }
                    // printf("block_num = %d, get_num = %d\n", block_num, get_num);
                    // for(int i = 0; i < get_num; i++) {
                    //     printf("shared addr: %p, rkey: %u\n", addr[i].addr, addr[i].rkey);
                    // }
                    block_num -= get_num;
                    fill_counter += get_num;
                    ring_cache->add_batch(addr, get_num);
                };
                return true;
                // if(new_cache_region(block_class)) {
                //     return fill_cache_block(block_class);
                //     // continue;
                // } else return false;
            }
        }
    } else {
        int request_block_num = class_cache_upper_bound[block_class] - ring_class_cache[block_class]->get_length();
        mr_rdma_addr addr[block_per_region];
        while(request_block_num > 0){
            int get_num;
            while((get_num = m_rdma_conn_->fetch_region_class_batch(current_class_region_[block_class], block_class, addr, request_block_num, false)) == 0){
                new_cache_region(block_class);
            }
            request_block_num -= get_num;
            fill_counter += (block_class+1)*get_num;
            ring_class_cache[block_class]->add_batch(addr, get_num);
            // for (int i = 0; i < get_num; i++) {
            //     ring_class_cache[block_class]->add_cache(addr[i]);
            //     // addr[i].addr = 0; addr[i].rkey = 0;
            //     fill_counter += (block_class+1);
            // }
        } ;
        // for(int i = 0; i<class_cache_upper_bound[block_class] - ring_class_cache[block_class]->get_length(); i++){
        //     mr_rdma_addr addr;

        //     while(!m_rdma_conn_->fetch_region_class_block(current_class_region_[block_class], block_class, addr.addr, 
        //         addr.rkey, false)) {
        //         // fetch new region
        //         // printf("no backup region, just fetch new region\n");
        //         new_cache_region(block_class);
        //     }
        //     // printf("fill class %d cache:%lx, %u\n", block_class, addr.addr, addr.rkey);
        //     // printf("region info:%lx,%x\n", current_class_region_[block_class].base_map_, current_class_region_[block_class].class_map_);
        //     ring_class_cache[block_class]->add_cache(addr);
        //     fill_counter += (block_class+1);
        // }
    }
    return true;
}

bool ComputingNode::fetch_mem_block(uint64_t &addr, uint32_t &rkey){
    mr_rdma_addr result;
    bool ret = ring_cache->force_fetch_cache(result);
    addr = result.addr; 
    // rkey = get_global_rkey();
    rkey = result.rkey;
    return ret;
}

bool ComputingNode::fetch_mem_class_block(uint16_t block_class, uint64_t &addr, uint32_t &rkey){
    mr_rdma_addr result;
    // bool ret = ring_class_cache[block_class]->force_fetch_cache(result);
    bool ret = ring_class_cache[block_class]->force_fetch_cache(result);
    // if(!ret){
    //     fill_cache_block(block_class);
    //     ret = ring_class_cache[block_class]->force_fetch_cache(result);
    // }
    addr = result.addr; rkey = result.rkey;
    return ret;
}

bool ComputingNode::free_mem_block(uint64_t addr){
    uint64_t region_offset = (addr - heap_start_) / region_size_;
    uint64_t region_block_offset = (addr - heap_start_) % region_size_ / block_size_;
    region_with_rkey* region;
    if(region_offset == current_region_->region.offset_) {
        region = current_region_;  
        if(!m_rdma_conn_->remote_rebind(addr, region->region.block_class_, region->rkey[region_block_offset])){
            region->region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
            return true;
        }  
        return false;
    }
    else if( exclusive_region_[region_offset].region.exclusive_ == 0 ){
            // printf("Not a local single block free, try to free the remote metadata\n");
        int free_class;
        if((free_class = m_rdma_conn_->free_region_block(addr, false)) == -1){
            printf("Free the remote metadata failed\n");
            return false;
        }
        if(free_class == -2 && region_offset == backup_region_.offset_){
            new_backup_region();
        } else if(region_offset == backup_region_.offset_) {
            backup_region_.base_map_ &= ~(uint32_t)(1<<region_block_offset);  
        }
        if(free_class == -2) free_class = 1;
        fill_counter -= (free_class+1);
        return true;
    } else {
    // m_rdma_conn_->remote_memzero(addr, (region->region.block_class_+1)*block_size_);
        region = &exclusive_region_[region_offset];
        if(!m_rdma_conn_->remote_rebind(addr, region->region.block_class_, region->rkey[region_block_offset])){
            region->region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
            if(free_bit_in_bitmap32(region->region.base_map_) == block_per_region) {
                auto result = free_region_.find(region);
                if (result != free_region_.end()){
                    free_region_.erase(result);
                }
                m_rdma_conn_->set_region_empty(region->region);
                region->region.exclusive_ = 0;
            }
            else if(free_bit_in_bitmap32(region->region.base_map_) == block_per_region/2){
                if(current_region_->region.base_map_ == bitmap32_filled){
                    auto result = free_region_.find(region);
                    if (result != free_region_.end()){
                        free_region_.erase(result);
                    }
                    current_region_ = region;
                }
                else {
                    free_region_.insert(region);
                }
            }
            // mr_rdma_addr value(addr, region->rkey[region_block_offset]);
            // printf("free cache addr:%lx, rkey:%u\n", value.addr, value.rkey);
            // ring_cache->add_cache(value);
            return true;
        }
    }
    return false;
}

bool ComputingNode::fetch_mem_block_nocached(uint64_t &addr, uint32_t &rkey){
    while(!m_rdma_conn_->fetch_region_block(current_region_->region, addr,rkey, false)) {
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