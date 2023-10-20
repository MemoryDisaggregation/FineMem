/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-11 16:42:26
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-25 17:00:15
 * @FilePath: /rmalloc_newbase/include/free_block_manager.h
 * @Description: Buddy tree for memory management 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <bits/stdint-uintn.h>
#include <cstdio>
#include <queue>
#include <mutex>

namespace mralloc {

class FreeBlockManager{
public:
struct remote_addr {
    uint64_t addr;
    uint32_t rkey;
} ;
    FreeBlockManager(uint64_t fast_size): fast_size_(fast_size) {};
    ~FreeBlockManager() {};
    virtual bool init(uint64_t addr, uint64_t size, uint32_t rkey) {return true;};
    virtual bool fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) {};
    virtual bool return_back(uint64_t addr, uint64_t size, uint32_t rkey) {return 0;};
    virtual bool fetch_fast(uint64_t &addr, uint32_t &rkey) {};
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
    
    bool init(uint64_t addr, uint64_t size, uint32_t rkey) override;

    bool fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) override;

    bool return_back(uint64_t addr, uint64_t size, uint32_t rkey) override;

    bool fetch_fast(uint64_t &addr, uint32_t &rkey) override;

    void print_state() override;
    
private:
    std::queue<remote_addr> free_fast_queue;

    const uint64_t queue_watermark = 1 << 30;

    uint64_t raw_heap; 

    uint64_t raw_size;

    std::mutex m_mutex_;

    uint64_t total_used;

    uint32_t raw_rkey;
    
};

} // namespace mralloc