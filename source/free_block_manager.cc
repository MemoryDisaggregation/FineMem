/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:42:48
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-14 17:33:02
 * @FilePath: /rmalloc_newbase/source/free_block_manager.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#include "free_block_manager.h"
#include <bits/stdint-uintn.h>
#include <sys/types.h>
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>

namespace mralloc {
 
    bool ClientBlockManager::init(uint64_t addr, uint64_t size, uint32_t rkey){
        if (size % block_size_ != 0) {
            printf("Error: FreeQueueManager only support size that is multiple of %ld \n", block_size_);
            return false;
        }
        size_ = size;
        if(size > 0) {
            for(uint64_t i = 0; i < size / block_size_; i++) {
                free_block_queue_.push({addr + i * block_size_, rkey});
                size_ += block_size_;
            }
        }
        total_used_ = 0;
        return true;
    }

    bool ClientBlockManager::fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(size == block_size_){
            return fetch_block(addr, rkey);
        } else {
            perror("alloc failed, no free space\n");
            return false;
        }
    }

    bool ClientBlockManager::fill_block(uint64_t addr, uint64_t size, uint32_t rkey) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        for(uint64_t i = 0; i < size / block_size_; i++){
            free_block_queue_.push({addr + i * block_size_, rkey});
        }
        return true;    
    }

    bool ClientBlockManager::fetch_block(uint64_t &addr, uint32_t &rkey){
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(free_block_queue_.empty()){
            perror("no enough cache!\n");
            return false;
        }
        remote_addr rem_addr = free_block_queue_.front();
        free_block_queue_.pop();
        total_used_ += block_size_;
        addr = rem_addr.addr; rkey = rem_addr.rkey;
        return true;
    }

    void ClientBlockManager::print_state() {
        printf("mem used: %lu MiB\n", total_used_/1024/1024);
    }

    bool ServerBlockManagerv2::init(uint64_t meta_addr, uint64_t addr, uint64_t size, uint32_t rkey) {
        uint64_t align = block_size_*large_block_items < 1024*1024*2 ? 1024*1024*2 : block_size_*large_block_items;
        uint64_t base_align = block_size_ < 1024*1024*2 ? 1024*1024*2 : block_size_;
        assert(meta_addr % base_align == 0 && addr % base_align == 0 && size % align == 0);
        large_block_num = size/align;
        block_info = (large_block*)meta_addr;
        heap_start = addr;
        heap_size = size;
        global_rkey = rkey;
        block_header_e header_; 
        header_.alloc_history = 0;
        header_.max_length = block_size_/base_size;
        header_.flag &= (uint32_t)0;
        header_.bitmap &= (uint32_t)0;
        free_list.store(nullptr);
        for(int i = large_block_num-1; i >= 0; i--){
            uint64_t bitmap_init = (uint64_t)0;
            block_info[i].bitmap.store(bitmap_init);
            for(int j = 0; j < large_block_items; j++){
                block_info[i].header[j].store(header_);
                block_info[i].rkey[j] = 0;
            }
            block_info[i].next = free_list.load();
            while(!free_list.compare_exchange_strong(block_info[i].next, &block_info[i]));
            block_info[i].offset = i;
        }
        last_alloc = 0;
        return true;
    }

    uint64_t find_free_index_from_bitmap(uint64_t bitmap) {
        return __builtin_ctzll(~bitmap);
    }

    bool ServerBlockManagerv2::fetch_block(uint64_t &addr, uint32_t &rkey) {
        uint64_t block_index, free_index;
        block_header_e header_old, header_new;
        large_block* free = free_list.load();
        while(free != nullptr) {
            // while((free_index = find_free_index_from_bitmap(free->bitmap.load())) != 64 && free->offset >= user_start) {
            //     // find valid bit, try to allocate
            //     header_old = free->header[free_index].load();
            //     if(header_old.flag % 2 == 1)
            //         continue;
            //     header_new = header_old; header_new.flag |= 1;
            //     if(!free->header[free_index].compare_exchange_strong(header_old, header_new)){
            //         continue;
            //     }
            //     free->bitmap.fetch_or((uint64_t)1<<free_index);
            //     addr = heap_start + (free->offset*large_block_items + free_index)*block_size_;
            //     rkey = global_rkey;
            //     return true;
            // }
            uint64_t bitmap_, bitmap_new_;
            if(free->offset >= user_start && (bitmap_ = free->bitmap.load()) !=  ~(uint64_t)0) {
                do{
                    bitmap_new_ = bitmap_;
                    free_index = find_free_index_from_bitmap(bitmap_);
                    bitmap_new_ |= (uint64_t)1<<free_index;
                    // printf("bitmap result = %lu\n", result);
                } while (!free->bitmap.compare_exchange_strong(bitmap_, bitmap_new_) && (bitmap_) != ~(uint64_t)0);
                if(bitmap_ != ~(uint64_t)0) {
                    // printf("block full\n");
                    addr = heap_start + (free->offset*large_block_items + free_index)*block_size_;
                    rkey = free->rkey[free_index];
                    return true;
                }
            }
            free_list.compare_exchange_strong(free, free->next);
        }
        printf("freelist failed!\n");
        for(int i = 0;i < large_block_num; i++){
            block_index = (i + last_alloc ) % large_block_num;  
            // load bitmap, and check if there are valid bit
            while((free_index = find_free_index_from_bitmap(block_info[block_index].bitmap.load())) != 64) {
                // find valid bit, try to allocate
                header_old = block_info[block_index].header[free_index].load();
                if(header_old.flag % 2 == 1)
                    continue;
                header_new = header_old; header_new.flag |= 1;
                if(!block_info[block_index].header[free_index].compare_exchange_strong(header_old, header_new)){
                    continue;
                }
                last_alloc = block_index;
                block_info[block_index].bitmap.fetch_or((uint64_t)1<<free_index);
                addr = heap_start + (block_index*large_block_items + free_index)*block_size_;
                rkey = block_info[block_index].rkey[free_index];
                return true;
            }
        }
        return false;
    }

    bool ServerBlockManagerv2::fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) {
        uint64_t start_index = get_block_index(start_addr);
        uint64_t end_index = get_block_index(start_addr + size - 1);
        for (int i = start_index; i <= end_index; i++) {
            block_header_e header_old = block_info[i/64].header[i%64].load();
            if (header_old.flag % 2 == 1 || (header_old.bitmap | (uint32_t)0) != 0){
                printf("Fixed start addr align malloc failed!\n");
                return false;
            }
        }
        std::unique_lock<std::mutex> lock(m_mutex_);
        for (int i = start_index; i <= end_index; i++) {
            block_header_e header_old = block_info[i/64].header[i%64].load();
            block_header_e header_new = header_old;
            assert(header_old.flag % 2 == 0);
            header_new.flag |= (uint64_t)1;
            if (!block_info[i/64].header[i%64].compare_exchange_strong(header_old, header_new)){
                printf("malloc failed!\n");
                return false;
            }
            block_info[i/64].bitmap.fetch_or((uint64_t)1<<(i%64));
        }
        rkey = global_rkey;
        addr = get_block_addr(start_index);
        user_start = end_index/64 + 1;
        printf("last alloc:%lu\n", last_alloc);
        return true;
        
    }

    bool ServerBlockManager::init(uint64_t addr, uint64_t size, uint32_t rkey) {
        large_block_num = size/block_size_;
        uint64_t metadata_size = large_block_num*sizeof(header_list);
        uint64_t rkey_size = large_block_num*sizeof(uint32_t);
        uint64_t block_align_offset = (metadata_size + rkey_size - 1)/block_size_*block_size_ + block_size_;
        large_block_num -= block_align_offset/block_size_;
        header_list = (block_header*)addr;
        block_header_e header_, old_header; 
        header_.alloc_history = 0;
        header_.max_length = block_size_/base_size;
        header_.flag |= (uint32_t)1;
        header_.bitmap &= (uint32_t)0;
        for(int i=0; i<large_block_num; i++){
            do{
                old_header = header_list[i].load();
            } while(!header_list[i].compare_exchange_weak(old_header, header_));
        }
        block_rkey_list = (uint32_t*)(addr + metadata_size);
        heap_start = addr + block_align_offset;
        heap_size = size - block_align_offset;
        global_rkey = rkey;
        last_alloc = 0;
        return true;
    }
    
    bool ServerBlockManager::fetch_block(uint64_t &addr, uint32_t &rkey) {
        block_header* header = get_metadata();
        uint64_t last_alloc_ = last_alloc.load();
        for(int i = 0; i< large_block_num-user_start; i++){
            uint64_t index = (i+last_alloc_)%(large_block_num - user_start) + user_start;
            block_header_e header_old = header[index].load();
            if(header_old.max_length == block_size_/base_size && (header_old.flag & (uint16_t)1) == 1){
                block_header_e update_header = header_old;
                update_header.flag &= ~((uint16_t)1);
                if(header[index].compare_exchange_strong(header_old, update_header)){
                    addr = get_block_addr(index);
                    rkey = global_rkey;
                    // rkey = get_block_rkey(index);
                    while(!last_alloc.compare_exchange_strong(last_alloc_, index + 1));
                    return true;
                }
            }
        }
        return false;
    }

    bool ServerBlockManager::fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) {
        uint64_t start_index = get_block_index(start_addr);
        uint64_t end_index = get_block_index(start_addr + size - 1);
        // uint64_t end_index = (start_addr + size - heap_start - 1)/block_size_ ;
        block_header* header = get_metadata();
        for (int i = start_index; i <= end_index; i++) {
            block_header_e header_old = header[i].load();
            if ((header_old.flag & (uint64_t)1) != 1 || (header_old.bitmap | (uint32_t)0) != 0){
                printf("Fixed start addr align malloc failed!\n");
                return false;
            }
        }
        std::unique_lock<std::mutex> lock(m_mutex_);
        for (int i = start_index; i <= end_index; i++) {
            block_header_e header_old = header[i].load();
            block_header_e header_new = header_old;
            assert((header_old.flag & (uint16_t)1) == (uint16_t)1);
            header_new.flag &= ~((uint64_t)1);
            if (!header[i].compare_exchange_strong(header_old, header_new)){
                printf("malloc failed!\n");
                return false;
            }
        }
        rkey = global_rkey;
        addr = get_block_addr(start_index);
        // last_alloc = end_index + 1;
        user_start = end_index + 1;
        printf("last alloc:%lu\n", last_alloc.load());
        return true;
    }

    bool FreeQueueManager::init(uint64_t addr, uint64_t size, uint32_t rkey){
        if (size % block_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld \n", block_size_);
            return false;
        }
        uint64_t cache_size = std::min(queue_watermark, size);
        raw_heap = addr;
        raw_size = size;
        raw_rkey = rkey;
        uint64_t start_addr = addr + raw_size - cache_size;
        for(uint64_t i = 0; i < cache_size / block_size_; i++){
            // free_block_queue.push({addr + raw_size - block_size_, rkey});
            free_block_queue.push({start_addr + i * block_size_, rkey});
            raw_size -= block_size_;
        }
        total_used = 0;
        return true;
    }

    bool FreeQueueManager::fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(size == block_size_){
            return fetch_block(addr, rkey);
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

    bool FreeQueueManager::fill_block(uint64_t addr, uint64_t size, uint32_t rkey) {
        std::unique_lock<std::mutex> lock(m_mutex_);
        // if (addr + size == raw_heap) {
        if (0) {
            raw_heap -= size;
            raw_size += size;
            // total_used -= size;
            return true;
        } else if (size % block_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld\n", block_size_);
            return false;
        }
        for(uint64_t i = 0; i < size / block_size_; i++){
            free_block_queue.push({addr + i * block_size_, rkey});
        }
        // total_used -= size;
        return true;    
    }

    bool FreeQueueManager::fetch_block(uint64_t &addr, uint32_t &rkey){
        std::unique_lock<std::mutex> lock(m_mutex_);
        if(free_block_queue.empty()){
            // for(uint64_t i = 0; i < 10; i++){
                if(raw_size >= block_size_){
                    free_block_queue.push({raw_heap + raw_size - block_size_, raw_rkey});
                    raw_size -= block_size_;
                } else {
                    // perror("no enough cache!\n");
                    return false;
                }
            // }
        }
        remote_addr rem_addr = free_block_queue.front();
        free_block_queue.pop();
        total_used += block_size_;
        // printf("mem used: %lu\n", total_used);
        addr = rem_addr.addr; rkey = rem_addr.rkey;
        return true;
    }

    void FreeQueueManager::print_state() {
        printf("mem used: %lu MiB\n", total_used/1024/1024);
    }

}