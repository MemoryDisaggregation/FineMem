/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:42:48
 * @LastEditors: blahaj wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-06 22:14:29
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
    
    bool ServerBlockManager::init(uint64_t meta_addr, uint64_t addr, uint64_t size, uint32_t rkey) {
        assert(size%region_size_ == 0);
        block_num_ = size/block_size_;
        region_num_ = block_num_/block_per_region;
        section_num_ = region_num_/region_per_section;
        global_rkey_ = rkey;

        section_header_ = (section*)meta_addr;
        fast_region_ = (fast_class*)(section_header_+ section_num_); 
        region_header_ = (region*)(fast_region_ + block_class_num*section_num_);
        block_rkey_ = (uint32_t*)(region_header_ + region_num_);
        class_block_rkey_ = (uint32_t*)(block_rkey_ + block_num_);
        
        heap_start_ = addr;
        heap_size_ = size;
        assert(heap_start_ > (uint64_t)(block_rkey_ + block_num_));

        if((uint64_t)(class_block_rkey_ + block_num_) > heap_start_) {
            printf("metadata out of bound\n");
        }

        section_e init_section_header = {0,0};
        region_e init_region_header = {0, 0, 0, 0};

        for(int i = 0; i < section_num_; i++) {
            section_header_[i].store(init_section_header);
        }
        fast_class_e null_class = {0,0,0,0};
        for(int i = 0; i < section_num_ ; i++) {
            for(int j = 0; j < block_class_num; j++) {
                fast_region_[i*block_class_num + j].store(null_class);
            }
        }
        
        for(int i = 0; i < region_num_; i++) {
            init_region_header.offset_ = i;
            init_region_header.exclusive_ = 0;
            region_header_[i].store(init_region_header);
        }

        for(int i = 0; i < block_num_; i++) {
            block_rkey_[i] = 0;
        }

        // // init: try to allocate each size on each section
        // for(int i = 0; i < section_num_/4; i++) {
        //     for(int j = 1; j < block_class_num; j ++) {
        //         section_e section = section_header_[i*4+3].load();
        //         region_e alloc_region;
        //         fetch_region(section, i*4, j, true, alloc_region);
        //         try_add_fast_region(i*4, j, alloc_region);
        //     }
        // }

        return true;
    }

    inline bool ServerBlockManager::check_section(section_e alloc_section, alloc_advise advise, uint32_t offset) {
        switch (advise) {
        case alloc_empty:
            return ((~alloc_section.alloc_map_ & ~alloc_section.class_map_) & 1<< offset) != 0;
        case alloc_no_class:
            return ((alloc_section.alloc_map_ & ~alloc_section.class_map_) & 1<< offset) != 0;
        case alloc_class:
            return ((~alloc_section.alloc_map_ & alloc_section.class_map_) & 1<< offset) != 0;
        case alloc_exclusive:
            return ((alloc_section.alloc_map_ & alloc_section.class_map_) & 1<< offset) != 0;
        }   
    }

    bool ServerBlockManager::update_section(region_e region, alloc_advise advise, alloc_advise compare) {
        uint64_t section_offset = region.offset_/region_per_section;
        uint64_t region_offset = region.offset_%region_per_section;
        section_e section_old = section_header_[section_offset].load();
        section_e section_new = section_old;
        if(advise == alloc_exclusive) {
            do{
                if(!check_section(section_old, compare, region_offset)){
                    return false;
                }
                section_new.alloc_map_ |= (uint32_t)1 << region_offset;
                section_new.class_map_ |= (uint32_t)1 << region_offset;
            }while(!section_header_[section_offset].compare_exchange_strong(section_old, section_new));
            return true;
        } else if(advise == alloc_empty) {
            do{
                if(!check_section(section_old, compare, region_offset)){
                    return false;
                }
                section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
                section_new.class_map_ &= ~((bitmap32)1 << region_offset);
            }while(!section_header_[section_offset].compare_exchange_strong(section_old, section_new));
            return true;
        } else if(advise == alloc_no_class) {
            do{
                if(!check_section(section_old, compare, region_offset)){
                    return false;
                }
                section_new.class_map_ &= ~((bitmap32)1 << region_offset);
                section_new.alloc_map_ |= (uint32_t)1 << region_offset;
            }while(!section_header_[section_offset].compare_exchange_strong(section_old, section_new));
            return true;
        } else if(advise == alloc_class) {
            do{
                if(!check_section(section_old, compare, region_offset)){
                    return false;
                }
                section_new.class_map_ |= (uint32_t)1 << region_offset;
                section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
            }while(!section_header_[section_offset].compare_exchange_strong(section_old, section_new));
            return true;
        }
        return false;
    }

    bool ServerBlockManager::find_section(section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) {
        section_e section;
        if(advise == alloc_class) {
            for(int i = 0; i < section_num_; i++) {
                section = section_header_[i].load();
                if((section.class_map_ | section.alloc_map_) != ~(uint32_t)0){
                    alloc_section = section;
                    section_offset = i;
                    return true;
                }
            }
        } else if(advise == alloc_no_class) {
            for(int i = 0; i < section_num_; i++) {
                section = section_header_[i].load();
                if((section.class_map_ & section.alloc_map_)  != ~(uint32_t)0){
                    alloc_section = section;
                    section_offset = i;
                    return true;
                }
            }
        } else if (advise == alloc_empty) {
            for(int i = 0; i < section_num_; i++) {
                section = section_header_[i].load();
                if((section.class_map_ | section.alloc_map_ ) != ~(uint32_t)0){
                    alloc_section = section;
                    section_offset = i;
                    return true;
                }
            }
        } else { return false; }
        return false;
    }

    bool ServerBlockManager::fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region) {
        if(block_class == 0 && shared == true) {
            // force use unclassed one to alloc single block
            section_e new_section;
            uint32_t free_map;
            int index;
            do {
                free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
                if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                    free_map = alloc_section.class_map_;
                    if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                        printf("section has no free space!\n");
                        return false;
                    }
                }
                // if first alloc, do state update
                if(((alloc_section.alloc_map_>>index) & 1) != 0) {
                    break;
                }
                new_section = alloc_section;
                new_section.alloc_map_ |= ((uint32_t)1<<index);
            }while (!section_header_[section_offset].compare_exchange_strong(alloc_section, new_section));
            alloc_section = new_section;
            alloc_region = region_header_[section_offset*region_per_section+index].load();
            return true;
        }
        else if(block_class == 0 && shared == false) {
            section_e new_section;
            uint32_t free_map;
            int index;
            do {
                free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
                // search exclusive block, from the tail
                if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
                new_section = alloc_section;
                new_section.alloc_map_ |= ((uint32_t)1<<index);
                new_section.class_map_ |= ((uint32_t)1<<index);
            }while (!section_header_[section_offset].compare_exchange_strong(alloc_section, new_section));
            alloc_section = new_section;
            region_e region_old= region_header_[section_offset*region_per_section+index].load();
            region_e region_new;
            do {
                region_new = region_old;
                if(region_new.exclusive_ == 1) {
                    printf("impossible problem: exclusive is already set\n");
                    return false;
                }
                region_new.exclusive_ = 1;
            } while (!region_header_[region_new.offset_].compare_exchange_strong(region_old, region_new));
            region_old = region_new;
            alloc_region = region_old;
            return true;
        }
        // class alloc, shared
        else if (shared == true) {
            int index;
            uint32_t fast_index = get_fast_region_index(section_offset, block_class);
            region_e region_new, region_old;
            fast_class_e class_region = fast_region_[fast_index].load();
            for(int i = 0; i < 4;i++){
                if((index = class_region.offset[i]) != 0){
                    region_old = region_header_[index].load();
                    if(region_old.exclusive_ == 0 && region_old.block_class_ == block_class && region_old.class_map_ != bitmap16_filled) {
                        alloc_region = region_old;
                        return true;
                    } else {
                        fast_class_e new_class_region;
                        new_class_region = class_region;
                        new_class_region.offset[i] = 0;
                        if(fast_region_[fast_index].compare_exchange_strong(class_region, new_class_region)){
                            class_region = new_class_region;
                        }
                    }
                }
            }
            uint32_t free_map;
            section_e new_section;
            do {
                free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
                // search class block, from the tail
                if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
                new_section = alloc_section;
                new_section.class_map_ |= ((uint32_t)1<<index);
                new_section.alloc_map_ &= ~((uint32_t)1<<index);
            }while (!section_header_[section_offset].compare_exchange_strong(alloc_section, new_section));
            alloc_section = new_section;
            region_old= region_header_[section_offset*region_per_section+index].load();
            init_region_class(region_old, block_class, 0);
            try_add_fast_region(section_offset, block_class, region_old);
            alloc_region = region_old;
            return true;
        }
        // class alloc, and exclusive
        else {
            int index;
            region_e region_old, region_new;
            uint32_t free_map;
            section_e new_section;
            do {
                free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
                // search class block, from the tail
                if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
                new_section = alloc_section;
                new_section.class_map_ |= ((uint32_t)1<<index);
                new_section.alloc_map_ |= ((uint32_t)1<<index);
            }while (!section_header_[section_offset].compare_exchange_strong(alloc_section, new_section));
            alloc_section = new_section;
            region_old= region_header_[section_offset*region_per_section+index].load();
            init_region_class(region_old, block_class, 1);
            try_add_fast_region(section_offset, block_class, region_old);
            alloc_region = region_old;
            return true;
        }
        return false;
    }

    bool ServerBlockManager::try_add_fast_region(uint32_t section_offset, uint32_t block_class, region_e &alloc_region){
        bool replace = true; uint32_t fast_index = get_fast_region_index(section_offset, block_class);
        fast_class_e class_region = fast_region_[fast_index].load();
        fast_class_e new_class_region = class_region;
        for(int i = 0; i < 4;i++){
            if (class_region.offset[i] == alloc_region.offset_) {
                return true;
            }
        }
        for(int i = 0; i < 4;i++){
            if(class_region.offset[i] == 0) {
                replace = true;
                do {
                    if(class_region.offset[i] != 0){
                        replace = false;
                        break;
                    }
                    new_class_region = class_region;
                    new_class_region.offset[i] = alloc_region.offset_;
                }while(!(fast_region_[fast_index].compare_exchange_strong(class_region, new_class_region)));
                if(replace) return true;
            }
        }
        return false;
    }

    bool ServerBlockManager::fetch_large_region(section_e &alloc_section, uint32_t section_offset, uint64_t region_num, uint64_t &addr) {
        bitmap32 free_map = alloc_section.alloc_map_ | alloc_section.class_map_;
        int free_length = 0;
        // each section has 32 items
        for(int i = 0; i < 32; i++) {
            // a free space
            if(free_map%2 == 0) {
                free_length += 1;
                // length enough
                if(free_length == region_num) {
                    section_e section_new = alloc_section;
                    bitmap32 mask = 0;
                    for(int j = i-free_length+1; j <= i; j++) {
                        mask |= (uint32_t)1 << j;
                    }
                    section_new.alloc_map_ |= mask;
                    section_new.class_map_ |= mask;
                    // find the section header changed
                    if(!section_header_[section_offset].compare_exchange_strong(alloc_section, section_new)){
                        i = 0; free_length = 0; free_map = alloc_section.alloc_map_ | alloc_section.class_map_;
                        continue;
                    }
                    alloc_section = section_new;
                    addr = get_section_region_addr(section_offset, i-free_length+1);
                    return true;
                }
            } else {
                free_length = 0;
            }
            free_map >>= 1;
        } 
        return false;
    }

    bool ServerBlockManager::set_region_exclusive(region_e &alloc_region) {
        if(!update_section(alloc_region, alloc_exclusive, alloc_empty)){
            return false;
        }
        region_e new_region;
        do {
            new_region = alloc_region;
            if(new_region.exclusive_ == 1) {
                printf("impossible situation: exclusive has already been set\n");
                return false;
            }
            new_region.exclusive_ = 1;
        } while(!region_header_[new_region.offset_].compare_exchange_strong(alloc_region, new_region));
        alloc_region = new_region;
        return true;
    }
    bool ServerBlockManager::set_region_empty(region_e &alloc_region) {
        if(alloc_region.exclusive_ != 1) {
            if(!set_region_exclusive(alloc_region))
                return false;
        }
        region_e new_region;
        do {
            new_region = alloc_region;
            if(new_region.base_map_ != 0) {
                printf("wait for free\n");
                return false;
            }
            new_region.exclusive_ = 0;
            new_region.block_class_ = 0;
        } while(!region_header_[new_region.offset_].compare_exchange_strong(alloc_region, new_region));
        alloc_region = new_region;
        if(!update_section(alloc_region, alloc_empty, alloc_exclusive)){
            return false;
        }
        return true;
    }

    bool ServerBlockManager::init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive) {
        // suppose the section has already set!
        region_e new_region;
        do {
            new_region = alloc_region;
            if((alloc_region.block_class_ != 0 && alloc_region.block_class_ != block_class) || alloc_region.exclusive_ != is_exclusive)  {
                return false;
            } 
            uint16_t mask = 0;
            uint32_t reader = new_region.base_map_;
            uint32_t tail = (uint32_t)1<<(block_class+1);
            for(int i = 0; i < block_per_region/(block_class+1); i++ ) {
                if(reader%tail == 0)
                    mask |= (uint16_t)1<<i;
                reader >>= block_class+1;
            }
            new_region.class_map_ = ~mask;
            new_region.block_class_ = block_class;
        } while (!region_header_[new_region.offset_].compare_exchange_strong(alloc_region, new_region));
        alloc_region = new_region;
        return true;
    }

    bool ServerBlockManager::fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive) {
        int index; region_e new_region;
        do{
            if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ != 0) {
                printf("state wrong\n");
                return false;
            } 
            new_region = alloc_region;
            if((index = find_free_index_from_bitmap32_tail(alloc_region.base_map_)) == -1) {
                printf("full, change region\n");
                return false;
            }
            new_region.base_map_ |= (uint32_t)1<<index;
        } while (!region_header_[new_region.offset_].compare_exchange_strong(alloc_region, new_region));
        alloc_region = new_region;
        addr = get_region_block_addr(alloc_region, index);
        rkey = get_region_block_rkey(alloc_region, index);
        if(alloc_region.base_map_ == bitmap32_filled) {
            update_section(alloc_region, alloc_exclusive, alloc_no_class);
        }
        return true;
    }

    bool ServerBlockManager::fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive) {
        int index; region_e new_region;
        do {
            if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ == 0) {
                printf("already exclusive\n");
                return false;
            } 
            new_region = alloc_region;
            if((index = find_free_index_from_bitmap16_tail(alloc_region.class_map_)) == -1) {
                return false;
            }
            uint32_t mask = 0;
            for(int i = 0;i < block_class + 1;i++) {
                mask |= (uint32_t)1<<(index*(block_class + 1)+i);
            }
            new_region.base_map_ |= mask;
            new_region.class_map_ |= (uint16_t)1<<index;
        }while(!region_header_[new_region.offset_].compare_exchange_strong(alloc_region, new_region));
        alloc_region = new_region;
        addr = get_region_block_addr(alloc_region, index*(block_class + 1));
        rkey = get_region_class_block_rkey(alloc_region, index*(block_class + 1));
        return true;
    }

    int ServerBlockManager::free_region_block(uint64_t addr, bool is_exclusive) {
        uint32_t region_offset = (addr - heap_start_) / region_size_;
        uint32_t region_block_offset = (addr - heap_start_) % region_size_ / block_size_;
        region_e region = region_header_[region_offset].load();

        if(!region.exclusive_ && is_exclusive) {
            printf("exclusive error, the actual block is shared\n");
            return -1;
        }
        if((region.base_map_ & ((uint32_t)1<<region_block_offset)) == 0) {
            printf("already freed\n");
            return -1;
        }
        uint32_t new_rkey;
        if(region.block_class_ == 0) {
            region_e new_region;
            do{
                new_region = region;
                new_region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
            } while(!region_header_[region_offset].compare_exchange_strong(region, new_region));
            // printf("free: %p, region id:%u, bitmap from %x to %x\n", addr, new_region.offset_ , region.base_map_, new_region.base_map_);
            if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) > block_per_region/2 && free_bit_in_bitmap32(region.base_map_) <= block_per_region/2){
                update_section(new_region, alloc_no_class, alloc_exclusive);
            }
            region = new_region;
            return 0;
        } else {
            region_e new_region;
            uint16_t block_class = region.block_class_;
            do{
                new_region = region;
                uint32_t mask = 0; 
                for(int i = 0;i < block_class + 1;i++) {
                    mask |= (uint32_t)1<<(region_block_offset+i);
                }
                new_region.base_map_ &= ~mask;
                new_region.class_map_ &= ~(uint16_t)(1<<region_block_offset/(block_class+1));
            } while(!region_header_[region_offset].compare_exchange_strong(region, new_region));
            if(free_bit_in_bitmap16(new_region.class_map_) == block_per_region/(block_class+1)) {
                // printf("[Attention] try to add fast region %lx\n", addr);
                if(!try_add_fast_region(region_offset/region_per_section, block_class, new_region)) {
                    // printf("[Attention] try to clean region %lx\n", addr);
                    set_region_empty(new_region);
                }
            } else if(!is_exclusive && free_bit_in_bitmap16(new_region.class_map_) > block_per_region/(block_class+1)/2 && free_bit_in_bitmap16(region.class_map_) <= block_per_region/(block_class+1)/2){
                // printf("[Attention] try to add fast region %lx\n", addr);
                try_add_fast_region(region_offset/region_per_section, block_class, new_region);
            }
            region = new_region;
            // printf("free a class %u block %lx, newkey is %u\n", new_region.block_class_, addr, new_rkey);
            return block_class;
        }
        return -1;
    }

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

    bool ServerBlockManagerv1::init(uint64_t addr, uint64_t size, uint32_t rkey) {
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
    
    bool ServerBlockManagerv1::fetch_block(uint64_t &addr, uint32_t &rkey) {
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

    bool ServerBlockManagerv1::fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) {
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