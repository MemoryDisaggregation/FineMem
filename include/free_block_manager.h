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
    FreeBlockManager(uint64_t fast_size): fast_size_(fast_size) {};
    ~FreeBlockManager() {};
    virtual bool init(uint64_t addr, uint64_t size) {return true;};
    virtual uint64_t fetch(uint64_t size) {return 0;};
    virtual bool return_back(uint64_t addr, uint64_t size) {return 0;};
    virtual uint64_t fetch_fast() {return 0;};
    virtual void print_state() {};
    uint64_t get_fast_size() {return fast_size_;};
protected:
    uint64_t fast_size_;
};
    
class FreeQueueManager: public FreeBlockManager{
public:
    FreeQueueManager(uint64_t fast_size):FreeBlockManager(fast_size) {};
    ~FreeQueueManager() {
        while(!free_fast_queue.empty()){
            free_fast_queue.pop();
        }
    };
    
    bool init(uint64_t addr, uint64_t size) override;

    uint64_t fetch(uint64_t size) override;

    bool return_back(uint64_t addr, uint64_t size) override;

    uint64_t fetch_fast() override;

    void print_state() override{};
    
private:
    std::queue<uint64_t> free_fast_queue;

    const uint64_t queue_watermark = 1 << 30;

    uint64_t raw_heap; 

    uint64_t raw_size;
    
};

} // namespace mralloc