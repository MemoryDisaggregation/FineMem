/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:42:48
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-10-20 16:09:22
 * @FilePath: /rmalloc_newbase/source/free_block_manager.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#include "free_block_manager.h"
#include <bits/stdint-uintn.h>
#include <sys/types.h>
#include <algorithm>
#include <cstdio>
#include <cstdlib>

namespace mralloc {

    bool ClientBlockManager::init(uint64_t addr, uint64_t size, uint32_t rkey) {
        block_num = size/fast_size_;
        uint64_t metadata_size = block_num*sizeof(header_list);
        uint64_t rkey_size = block_num*sizeof(uint32_t);
        header_list = (block_header*)malloc(metadata_size);
        block_rkey_list = (uint32_t*)malloc(rkey_size);
        uint64_t block_align_offset = (metadata_size + rkey_size - 1)/fast_size_*fast_size_ + fast_size_;
        mm_header_addr = addr;
        mm_rkey_addr = addr + metadata_size;
        mm_heap_addr = addr + block_align_offset;
        heap_size = size - block_align_offset;
        global_rkey = rkey;
        last_alloc = 0;
        return true;
    }

    bool ServerBlockManager::init(uint64_t addr, uint64_t size, uint32_t rkey) {
        block_num = size/fast_size_;
        uint64_t metadata_size = block_num*sizeof(header_list);
        uint64_t rkey_size = block_num*sizeof(uint32_t);
        header_list = (block_header*)addr;
        for(int i=0; i<block_num; i++){
            header_list[i].alloc_history = 0;
            header_list[i].max_length = fast_size_/base_size;
            header_list[i].flag |= (uint32_t)1;
            header_list[i].bitmap &= (uint32_t)0;
        }
        block_rkey_list = (uint32_t*)(addr + metadata_size);
        uint64_t block_align_offset = (metadata_size + rkey_size - 1)/fast_size_*fast_size_ + fast_size_;
        heap_start = addr + block_align_offset;
        heap_size = size - block_align_offset;
        global_rkey = rkey;
        last_alloc = 0;
        return true;
    }
    
    bool ServerBlockManager::fetch_fast(uint64_t &addr, uint32_t &rkey) {
        block_header* header = get_metadata();
        for(int i = 0; i< block_num; i++){
            uint64_t index = (i+last_alloc)%block_num;
            if(header[index].max_length == fast_size_/base_size && (header[index].flag & (uint64_t)1) == 1){
                std::unique_lock<std::mutex> lock(m_mutex_);
                header[index].flag &= ~((uint64_t)1);
                lock.unlock();
                addr = get_block_addr(index);
                rkey = get_block_rkey(index);
                last_alloc = index;
                return true;
            }
        }
        return false;
    }

    bool ServerBlockManager::fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) {
        uint64_t start_index = get_block_index(start_addr);
        uint64_t end_index = (start_addr + size - heap_start - 1)/fast_size_ ;
        block_header* header = get_metadata();
        for (int i = start_index; i <= end_index; i++) {
            if ((header[i].flag & (uint64_t)1) != 1 || (header[i].bitmap | (uint32_t)0) != 0){
                printf("Fixed start addr align malloc failed!\n");
                return false;
            }
        }
        std::unique_lock<std::mutex> lock(m_mutex_);
        for (int i = start_index; i < end_index; i++) {
            header[i].flag &= ~((uint64_t)1);
        }
        rkey = global_rkey;
        addr = get_block_addr(start_index);
        return true;
    }

    bool FreeQueueManager::init(uint64_t addr, uint64_t size, uint32_t rkey){
        if (size % fast_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld \n", fast_size_);
            return false;
        }
        uint64_t cache_size = std::min(queue_watermark, size);
        raw_heap = addr;
        raw_size = size;
        raw_rkey = rkey;
        uint64_t start_addr = addr + raw_size - cache_size;
        for(uint64_t i = 0; i < cache_size / fast_size_; i++){
            // free_fast_queue.push({addr + raw_size - fast_size_, rkey});
            free_fast_queue.push({start_addr + i * fast_size_, rkey});
            raw_size -= fast_size_;
        }
        total_used = 0;
        return true;
    }

    bool FreeQueueManager::fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(size == fast_size_){
            return fetch_fast(addr, rkey);
        }
        else if (size <= raw_size) {
            uint64_t raw_alloc = raw_heap;
            raw_heap += size;
            raw_size -= size;
            total_used += size;
            addr = raw_alloc; rkey = raw_rkey;
            return true;
        } else {
            perror("alloc failed, no free space\n");
            return false;
        }
    }

    bool FreeQueueManager::return_back(uint64_t addr, uint64_t size, uint32_t rkey) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        // if (addr + size == raw_heap) {
        if (0) {
            raw_heap -= size;
            raw_size += size;
            // total_used -= size;
            return true;
        } else if (size % fast_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld\n", fast_size_);
            return false;
        }
        for(uint64_t i = 0; i < size / fast_size_; i++){
            free_fast_queue.push({addr + i * fast_size_, rkey});
        }
        // total_used -= size;
        return true;    
    }

    bool FreeQueueManager::fetch_fast(uint64_t &addr, uint32_t &rkey){
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(free_fast_queue.empty()){
            // for(uint64_t i = 0; i < 10; i++){
                if(raw_size >= fast_size_){
                    free_fast_queue.push({raw_heap + raw_size - fast_size_, raw_rkey});
                    raw_size -= fast_size_;
                } else {
                    // perror("no enough cache!\n");
                    return false;
                }
            // }
        }
        remote_addr rem_addr = free_fast_queue.front();
        free_fast_queue.pop();
        total_used += fast_size_;
        // printf("mem used: %lu\n", total_used);
        addr = rem_addr.addr; rkey = rem_addr.rkey;
        return true;
    }

    void FreeQueueManager::print_state() {
        printf("mem used: %lu MiB\n", total_used/1024/1024);
    }

}