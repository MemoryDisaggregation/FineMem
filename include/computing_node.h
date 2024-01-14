/*
 * @Author: blahaj wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:09:32
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-21 17:27:08
 * @FilePath: /rmalloc_newbase/include/memory_heap.h
 * @Description: memory heap for rmalloc
 */
#pragma once

#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <sched.h>
#include <atomic>
#include <set>
#include <unordered_map>
#include <sys/sysinfo.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include "msg.h"
#include "rdma_conn.h"
#include "rdma_conn_manager.h"
#include "string"
#include "thread"
#include "unordered_map"
#include "free_block_manager.h"
#include "cpu_cache.h"
#include "rpc_server.h"

namespace mralloc {

const uint32_t ring_buffer_size = 8192;
const uint32_t class_ring_buffer_size = 2048;


class ComputingNode {
public:
    typedef struct {
        uint64_t addr;
        uint32_t rkey;
    } rdma_mem_t;

    ComputingNode(bool heap_enabled, bool cache_enabled, bool one_side_enabled): heap_enabled_(heap_enabled), cpu_cache_enabled_(cache_enabled), one_side_enabled_(one_side_enabled) {
        if(cpu_cache_enabled_)  assert(heap_enabled_);
        ring_cache = new ring_buffer_atomic<mr_rdma_addr>(ring_buffer_size, ring_cache_content, mr_rdma_addr(-1, -1), &reader, &writer);
        ring_cache->clear();
        for(int i = 0; i<class_num; i++) {
            ring_class_cache[i] = new ring_buffer_atomic<mr_rdma_addr>(class_ring_buffer_size, ring_class_cache_content[i], mr_rdma_addr(-1, -1), &class_reader[i], &class_writer[i]);
            ring_class_cache[i]->clear();
        }
    }

    bool start(const std::string addr, const std::string port) ;

    void stop() ;

    bool alive() ;

    void run() {};

    ~ComputingNode() { destory(); }

    void pre_fetcher() ;
    void cache_filler() ;
    void print_cpu_cache() ;
    void recycler() ;
    
    inline void show_ring_length() {
        printf("ring length:%u\n", ring_cache->get_length());
        return ;
    }
    void increase_class_watermark(uint16_t block_class, int &upper_bound);
    void decrease_class_watermark(uint16_t block_class, int &upper_bound);
    void increase_watermark(int &upper_bound);
    void decrease_watermark(int &upper_bound);

    inline uint64_t get_region_block_addr(uint32_t region_index, uint32_t block_offset) {return heap_start_ + region_index * region_size_ + block_offset * block_size_;} ;
    bool new_cache_section(uint32_t block_class, alloc_advise advise);
    bool new_cache_region(uint32_t block_class);
    bool new_backup_region();
    bool new_backup_section();
    bool fill_cache_block(uint32_t block_class);

    bool fetch_mem_block_nocached(uint64_t &addr, uint32_t &rkey);
    bool fetch_mem_block(uint64_t &addr, uint32_t &rkey);
    bool free_mem_block(uint64_t addr);
    bool free_mem_block_slow(uint64_t addr);
    bool free_mem_batch(uint32_t region_offset, uint32_t free_map);
    bool free_mem_block_fast_batch(uint64_t *addr);

    bool fetch_mem_class_block(uint16_t block_class, uint64_t &addr, uint32_t &rkey);

    // << RPC block fetch >>
    bool fetch_mem_block_remote(uint64_t &addr, uint32_t &rkey);

    // << local heap/cache fetch >>
    void fetch_cache(uint8_t nproc, uint64_t &addr, uint32_t &rkey);

    // UNUSED
    bool mr_bind_remote(uint64_t size, uint64_t addr, uint32_t rkey, uint32_t &newkey);

    ConnectionManager* get_conn(){return m_rdma_conn_;};

    void set_global_rkey(uint32_t rkey) {
        global_rkey_ = rkey;
    }

    uint32_t get_global_rkey() {
        return global_rkey_;
    }

private:
    void destory(){};

    FreeBlockManager *free_queue_manager;
    uint8_t running;
    bool use_global_rkey_;
    uint32_t global_rkey_;
    
    pthread_t pre_fetch_thread_;
    pthread_t cache_fill_thread_;
    pthread_t recycle_thread_;

    uint64_t block_size_;
    uint64_t block_num_;
    uint64_t region_size_;
    uint64_t region_num_;
    uint64_t section_size_;
    uint64_t section_num_;
    uint64_t fill_counter = 0;

    // info before heap segment
    uint64_t section_header_;
    uint64_t section_class_header_;
    uint64_t region_header_;
    uint64_t block_rkey_;
    uint64_t class_block_rkey_;
    uint64_t heap_start_;
    uint64_t block_header_;
    uint64_t backup_rkey_;

    // << allocation metadata >>
    one_side_info m_one_side_info_;
    section_e current_section_;
    uint32_t current_section_index_;
    region_with_rkey* current_region_;
    region_with_rkey* exclusive_region_;
    std::set<region_with_rkey*> free_region_;
    section_e backup_section_;
    uint32_t backup_section_index_;
    region_e backup_region_;
    uint32_t backup_region_index_;
    int backup_counter = 0;
    int backup_cycle = -1;
    region_e current_class_region_[16];
    uint32_t current_class_region_index_[16];

    // << reserved block cache>>
    ring_buffer_atomic<mr_rdma_addr>* ring_cache;
    ring_buffer_atomic<mr_rdma_addr>* ring_class_cache[16];
    mr_rdma_addr ring_cache_content[ring_buffer_size];
    mr_rdma_addr ring_class_cache_content[16][class_ring_buffer_size];
    // rdma_mem_t ring_cache[ring_buffer_size];
    std::atomic<uint32_t> reader, writer;
    float cache_watermark_low;
    float cache_watermark_high;
    int cache_upper_bound;
    // rdma_mem_t ring_class_cache[16][class_ring_buffer_size];
    int class_cache_upper_bound[16];
    std::atomic<uint32_t> class_reader[16], class_writer[16];
    int cpu_cache_watermark[nprocs];
    int cpu_class_watermark[class_num];

    // << function enabled >>
    bool heap_enabled_;
    bool cpu_cache_enabled_;
    bool one_side_enabled_;

    // << cpu cache >>
    cpu_cache* cpu_cache_;
    std::atomic<uint8_t> heap_worker_id_;
    uint8_t heap_worker_num_;

    // << one-side metadata >>

    block_header_e* header_list;
    uint32_t* rkey_list;
    uint64_t last_alloc_;
    uint64_t hint_ = 0;
    ConnectionManager *m_rdma_conn_;
    std::vector<rdma_mem_t> m_used_mem_; /* the used mem */
    std::mutex m_mutex_;                 /* used for concurrent mem allocation */

     uint64_t time_stamp_;
};

}
