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
        if (size % BASE_BLOCK_SIZE != 0){
            printf("Error: FreeQueueManager only support size that is multiple of 2MB\n");
            return false;
        }
        for(uint64_t i = 0; i < size / BASE_BLOCK_SIZE; i++){
            free_2MB_queue.push(addr + i * BASE_BLOCK_SIZE);
        }
        return true;
    }

    uint64_t FreeQueueManager::fetch(uint64_t size) {
        if(size == BASE_BLOCK_SIZE){
            return fetch_2MB_fast();
        }
        else {
            printf("Error: FreeQueueManager does not support size other than 2MB\n");
            return 0;
        }
    }

    uint64_t FreeQueueManager::return_back(uint64_t addr, uint64_t size) {
        if(size == 1024*1024*2){
            free_2MB_queue.push(addr);
            return 0;
        }
        else {
            printf("Error: FreeQueueManager does not support size other than 2MB\n");
            return 0;
        }
    }

    uint64_t FreeQueueManager::fetch_2MB_fast(){
        if(free_2MB_queue.empty()){
            return 0;
        }
        uint64_t addr = free_2MB_queue.front();
        free_2MB_queue.pop();
        return addr;
    }


}