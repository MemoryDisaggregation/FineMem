/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-14 09:42:48
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-08-14 16:15:17
 * @FilePath: /rmalloc_newbase/source/free_block_manager.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#include "free_block_manager.h"

namespace mralloc {

    bool FreeQueueManager::init(uint64_t addr, uint64_t size){
        if (size % fast_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld \n", fast_size_);
            return false;
        }
        for(uint64_t i = 0; i < size / fast_size_; i++){
            free_fast_queue.push(addr + i * fast_size_);
        }
        return true;
    }

    uint64_t FreeQueueManager::fetch(uint64_t size) {
        if(size == fast_size_){
            return fetch_fast();
        }
        else {
            printf("Error: FreeQueueManager does not support size other than %ld\n", fast_size_);
            return 0;
        }
    }

    bool FreeQueueManager::return_back(uint64_t addr, uint64_t size) {
        if (size % fast_size_ != 0){
            printf("Error: FreeQueueManager only support size that is multiple of %ld\n", fast_size_);
            return false;
        }
        for(uint64_t i = 0; i < size / fast_size_; i++){
            free_fast_queue.push(addr + i * fast_size_);
        }
        return true;    
    }

    uint64_t FreeQueueManager::fetch_fast(){
        if(free_fast_queue.empty()){
            return 0;
        }
        uint64_t addr = free_fast_queue.front();
        free_fast_queue.pop();
        return addr;
    }


}