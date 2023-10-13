/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:42:48
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-25 17:01:07
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

namespace mralloc {

    bool FreeQueueManager::init(uint64_t addr, uint64_t size){
        if (size % fast_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld \n", fast_size_);
            return false;
        }
        uint64_t cache_size = std::min(queue_watermark, size);
        raw_heap = addr;
        raw_size = size;
        for(uint64_t i = 0; i < cache_size / fast_size_; i++){
            free_fast_queue.push(addr + raw_size - fast_size_);
            raw_size -= fast_size_;
        }
        total_used = 0;
        return true;
    }

    uint64_t FreeQueueManager::fetch(uint64_t size) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(size == fast_size_){
            return fetch_fast();
        }
        else if (size <= raw_size) {
            uint64_t raw_alloc = raw_heap;
            raw_heap += size;
            raw_size -= size;
            total_used += size;
            return raw_alloc;
        } else {
            perror("alloc failed, no free space\n");
            return 0;
        }
    }

    bool FreeQueueManager::return_back(uint64_t addr, uint64_t size) {
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
            free_fast_queue.push(addr + i * fast_size_);
        }
        // total_used -= size;
        return true;    
    }

    uint64_t FreeQueueManager::fetch_fast(){
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(free_fast_queue.empty()){
            // for(uint64_t i = 0; i < 10; i++){
                if(raw_size >= fast_size_){
                    free_fast_queue.push(raw_heap + raw_size - fast_size_);
                    raw_size -= fast_size_;
                } else {
                    // perror("no enough cache!\n");
                    return 0;
                }
            // }
        }
        uint64_t addr = free_fast_queue.front();
        free_fast_queue.pop();
        total_used += fast_size_;
        // printf("mem used: %lu\n", total_used);
        return addr;
    }

    void FreeQueueManager::print_state() {
        printf("mem used: %lu\n", total_used);
    }

}