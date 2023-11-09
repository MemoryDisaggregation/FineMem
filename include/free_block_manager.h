/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-11 16:42:26
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-08 15:16:45
 * @FilePath: /rmalloc_newbase/include/free_block_manager.h
 * @Description: Buddy tree for memory management 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <bits/stdint-uintn.h>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <queue>
#include <mutex>

namespace mralloc {

const uint64_t large_block_items = 64;

struct block_header_e {
    uint8_t max_length;
    uint8_t alloc_history;
    uint16_t flag;
    uint32_t bitmap;
};

typedef std::atomic<block_header_e> block_header;
typedef std::atomic<uint64_t> bitmap64;

struct large_block {
    bitmap64 bitmap;
    block_header header[large_block_items];
    uint32_t rkey[large_block_items];
    large_block* next;
    uint64_t offset;
};

struct large_block_lockless {
    uint64_t bitmap;
    block_header_e header[large_block_items];
    uint32_t rkey[large_block_items];
    large_block* next;
    uint64_t offset;
};

class FreeBlockManager{
public:
struct remote_addr {
    uint64_t addr;
    uint32_t rkey;
} ;
    FreeBlockManager(uint64_t fast_size): fast_size_(fast_size) {};
    ~FreeBlockManager() {};
    virtual bool init(uint64_t addr, uint64_t size, uint32_t rkey) {return true;};
    virtual bool init(uint64_t meta_addr, uint64_t addr, uint64_t size, uint32_t rkey) {return true;};
    virtual void init_size_align(uint64_t addr, uint64_t size, uint64_t &init_addr, uint64_t &init_size) {};
    virtual bool fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) {return true;};
    virtual bool fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) {return true;};
    virtual bool return_back(uint64_t addr, uint64_t size, uint32_t rkey) {return 0;};
    virtual bool fetch_fast(uint64_t &addr, uint32_t &rkey) {return true;};
    virtual void print_state() {};
    uint64_t get_fast_size() {return fast_size_;};
protected:
    uint64_t fast_size_;
};

class ServerBlockManagerv2: public FreeBlockManager{
public:
    ServerBlockManagerv2(uint64_t block_size, uint64_t base_size):FreeBlockManager(block_size), base_size(base_size) {
        if(fast_size_/base_size > 32) {
            printf("bitmap cannot store too much bsae page!\n");
        }
    };
    ~ServerBlockManagerv2() {};
    
    inline uint64_t num_align_upper(uint64_t num, uint64_t align) {
        return (num + align - 1) - ((num + align - 1) % align);
    }

    void init_size_align(uint64_t addr, uint64_t size, uint64_t &init_addr, uint64_t &init_size) override {
        uint64_t align = fast_size_ * large_block_items;
        init_size = num_align_upper(size, align);
        uint64_t block_header_size = num_align_upper(init_size / align * sizeof(large_block), align);
        init_size += block_header_size;
        init_addr = addr - block_header_size;
        assert(init_addr % align == 0);
    };

    bool init(uint64_t meta_addr, uint64_t addr, uint64_t size, uint32_t rkey) override;

    inline bool set_block_rkey(uint64_t index, uint32_t rkey) {block_info[index / large_block_items].rkey[index % large_block_items] = rkey; return true;};

    bool set_block_base_rkey(uint64_t index, uint64_t offset, uint32_t rkey) {
        perror("unimplemented!\n");
        return false;
    };

    inline uint64_t get_base_num() {return fast_size_/base_size;};

    inline uint64_t get_block_num() {return block_num;};

    inline uint64_t get_block_addr(uint64_t index) {return heap_start + index * fast_size_;};

    inline uint64_t get_block_addr() {return heap_start;};

    // inline block_header get_block_header(uint64_t index) {return header_list[index];};

    inline large_block* get_metadata() {return block_info;};

    inline uint32_t get_block_rkey(uint64_t index) {return block_info[index / large_block_items].rkey[index % large_block_items];};

    inline uint64_t get_block_index(uint64_t addr) {return (addr-heap_start)/fast_size_;}

    inline uint64_t get_base_size() {return base_size;};

    bool fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) override {return true;};

    bool fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) override;

    bool return_back(uint64_t addr, uint64_t size, uint32_t rkey) override {return true;};

    bool fetch_fast(uint64_t &addr, uint32_t &rkey) override ;

    void print_state() override {};
    
private:

    std::mutex m_mutex_;

    uint64_t base_size;

    uint32_t global_rkey;

    large_block* block_info;

    std::atomic<large_block*> free_list;

    uint64_t heap_start;

    uint64_t heap_size;

    uint64_t block_num;

    uint64_t last_alloc;

    uint64_t user_start;
    
};

class ServerBlockManager: public FreeBlockManager{
public:
    ServerBlockManager(uint64_t block_size, uint64_t base_size):FreeBlockManager(block_size), base_size(base_size) {
        if(fast_size_/base_size > 32) {
            printf("bitmap cannot store too much bsae page!\n");
        }
    };
    ~ServerBlockManager() {};
    
    bool init(uint64_t addr, uint64_t size, uint32_t rkey) override ;

    inline bool set_block_rkey(uint64_t index, uint32_t rkey) {block_rkey_list[index] = rkey; return true;};

    bool set_block_base_rkey(uint64_t index, uint64_t offset, uint32_t rkey) {
        uint64_t start_addr = get_block_addr(index);
        *(uint32_t*)(start_addr + offset*base_size) = rkey;
        return true;
    };

    inline uint64_t get_base_num() {return fast_size_/base_size;};

    inline uint64_t get_block_num() {return block_num;};

    inline uint64_t get_block_addr(uint64_t index) {return heap_start + index * fast_size_;};

    inline uint64_t get_block_addr() {return heap_start;};

    // inline block_header get_block_header(uint64_t index) {return header_list[index];};

    inline block_header* get_metadata() {return header_list;};

    inline uint32_t get_block_rkey(uint64_t index) {return block_rkey_list[index];};

    inline uint64_t get_block_index(uint64_t addr) {return (addr-heap_start)/fast_size_;}

    inline uint64_t get_base_size() {return base_size;};

    inline uint64_t get_rkey_list_addr() {return (uint64_t)block_rkey_list;};

    bool fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) override {return true;};

    bool fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) override;

    bool return_back(uint64_t addr, uint64_t size, uint32_t rkey) override {return true;};

    bool fetch_fast(uint64_t &addr, uint32_t &rkey) override ;

    void print_state() override {};
    
private:

    std::mutex m_mutex_;

    uint64_t base_size;

    uint32_t global_rkey;

    block_header* header_list;

    uint32_t* block_rkey_list;

    uint64_t heap_start;

    uint64_t heap_size;

    uint64_t block_num;

    std::atomic<uint64_t> last_alloc;

    uint64_t user_start;
    
};

class ClientBlockManager: public FreeBlockManager{
public:

    ClientBlockManager(uint64_t block_size, uint64_t base_size):FreeBlockManager(block_size), base_size(base_size) {
        if(fast_size_/base_size > 32) {
            printf("bitmap cannot store too much bsae page!\n");
        }
    };
    ~ClientBlockManager() {};
    
    bool init(uint64_t addr, uint64_t size, uint32_t rkey) override {};

    // bool init(uint64_t header_addr, uint64_t rkey_addr, uint64_t base_addr, uint64_t size, uint32_t rkey) {return true;};

    inline bool set_block_rkey(uint64_t index, uint32_t rkey) {block_rkey_list[index] = rkey; return true;};

    bool set_block_base_rkey(uint64_t index, uint64_t offset, uint32_t rkey) {
        uint64_t start_addr = get_block_addr(index);
        // assert(get_base_num() == size);
        // for(int i = 0; i< size;i++)
        *(uint32_t*)(start_addr + offset*base_size) = rkey;
        return true;
    };

    inline uint64_t get_base_num() {return fast_size_/base_size;};

    inline uint64_t get_block_num() {return block_num;};

    inline uint64_t get_block_addr(uint64_t index) {return mm_header_addr + index * fast_size_;};

    // inline block_header get_block_header(uint64_t index) {return header_list[index];};

    inline block_header* get_metadata() {return header_list;};

    inline uint32_t get_block_rkey(uint64_t index) {return block_rkey_list[index];};

    inline uint64_t get_block_index(uint64_t addr) {return (addr-mm_heap_addr)/fast_size_;}

    bool fetch(uint64_t size, uint64_t &addr, uint32_t &rkey) override {return true;};

    bool fetch(uint64_t start_addr, uint64_t size, uint64_t &addr, uint32_t &rkey) override {return true;};

    bool return_back(uint64_t addr, uint64_t size, uint32_t rkey) override {return true;};

    bool fetch_fast(uint64_t &addr, uint32_t &rkey) override {return true;};

    void print_state() override {};
    
private:

    std::mutex m_mutex_;

    uint64_t base_size;

    uint32_t global_rkey;

    uint64_t mm_header_addr;

    uint64_t mm_rkey_addr;

    uint64_t mm_heap_addr;

    block_header* header_list;

    uint32_t* block_rkey_list;

    uint64_t heap_size;

    uint64_t block_num;

    uint64_t last_alloc;
    
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

    const uint64_t queue_watermark = (uint64_t)1 << 30;

    uint64_t raw_heap; 

    uint64_t raw_size;

    std::mutex m_mutex_;

    uint64_t total_used;

    uint32_t raw_rkey;
    
};

} // namespace mralloc