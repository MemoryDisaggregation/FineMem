/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-11 16:42:26
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-08-14 16:14:27
 * @FilePath: /rmalloc_newbase/include/free_block_manager.h
 * @Description: Buddy tree for memory management 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <bits/stdint-uintn.h>
#include <cstdio>
#include <queue>
namespace mralloc {

class FreeBlockManager{
public:
    FreeBlockManager() {};
    ~FreeBlockManager() {};
    virtual uint64_t fetch(uint64_t size) {return 0;};
    virtual uint64_t return_back(uint64_t addr, uint64_t size) {return 0;};
    virtual uint64_t fetch_2MB_fast() {return 0;};
    virtual void print_state() {};
protected:
    const uint64_t BASE_BLOCK_SIZE = 1024*1024*2;
};

class FreeQueueManager: public FreeBlockManager{
public:
    FreeQueueManager() {};
    ~FreeQueueManager() {
        while(!free_2MB_queue.empty()){
            free_2MB_queue.pop();
        }
    };
    
    bool init(uint64_t addr, uint64_t size);

    uint64_t fetch(uint64_t size) override;

    uint64_t return_back(uint64_t addr, uint64_t size) override;

    uint64_t fetch_2MB_fast() override;

    void print_state() override{};
    
private:
    std::queue<uint64_t> free_2MB_queue;
    
};

class FreeBuddyManager: public FreeBlockManager{
public:
    FreeBuddyManager() {};
    ~FreeBuddyManager() {};
    
    bool init(uint64_t addr, uint64_t size);

    uint64_t fetch(uint64_t size) override;

    uint64_t return_back(uint64_t addr, uint64_t size) override;

    uint64_t fetch_2MB_fast() override;

    void print_state() override;

// A buddy tree's private member:


};
    
} // namespace mralloc