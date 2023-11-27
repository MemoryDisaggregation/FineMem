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
#include <queue>
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

const uint32_t ring_buffer_size = 1024;
const uint32_t class_ring_buffer_size = 128;


class ComputingNode {
public:
    typedef struct {
        uint64_t addr;
        uint32_t rkey;
    } rdma_mem_t;

    ComputingNode(bool heap_enabled, bool cache_enabled, bool one_side_enabled): heap_enabled_(heap_enabled), cpu_cache_enabled_(cache_enabled), one_side_enabled_(one_side_enabled) {
        if(cpu_cache_enabled_)  assert(heap_enabled_);
    }

    bool start(const std::string addr, const std::string port) ;

    void stop() ;

    bool alive() ;

    void run() {};

    ~ComputingNode() { destory(); }

    void pre_fetcher() ;
    void cache_filler() ;
    void recycler() ;

    inline uint64_t ring_buffer_length() {
        if(reader == writer){
            return 0;
        } else {
            return reader > writer ? ring_buffer_size - reader + writer : writer - reader;
        }
    };
    
    inline void show_ring_length() {
        printf("ring length:%lu\n", ring_buffer_length());
        return ;
    }

    inline uint64_t get_region_block_addr(region_e region, uint32_t block_offset) {return heap_start_ + region.offset_ * region_size_ + block_offset * block_size_;} ;
    bool new_cache_section(uint32_t block_class);
    bool new_cache_region(uint32_t block_class);
    bool fill_cache_block(uint32_t block_class);

    bool fetch_mem_block_nocached(uint64_t &addr, uint32_t &rkey);
    bool fetch_mem_block(uint64_t &addr, uint32_t &rkey);
    bool free_mem_block(uint64_t addr);

    bool fetch_mem_class_block(uint64_t &addr, uint32_t &rkey);
    inline bool add_ring_cache(uint64_t addr, uint32_t rkey) {
        if(ring_buffer_length() < ring_buffer_size - 1&& ring_cache[writer].addr == 0 && ring_cache[writer].rkey == 0) {
            ring_cache[writer].addr = addr;
            ring_cache[writer].rkey = rkey;
            writer = (writer+1) % ring_buffer_size;
            return true;
        }
        return false;
    }

    // << one-sided block fetch >>
    // bool update_mem_metadata();
    // bool update_rkey_metadata();
    bool fetch_mem_block_one_sided(uint64_t &addr, uint32_t &rkey);
    // bool fetch_rkey_list_one_sided(uint64_t addr, uint32_t* rkey_list);

    // << RPC block fetch >>
    bool fetch_mem_block_remote(uint64_t &addr, uint32_t &rkey);

    // << local heap/cache fetch >>
    void fetch_cache(uint8_t nproc, uint64_t &addr, uint32_t &rkey);
    bool fetch_mem_block_local(uint64_t &addr, uint32_t &rkey);

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

    // info before heap segment
    uint64_t section_header_;
    uint64_t fast_region_;
    uint64_t region_header_;
    uint64_t block_rkey_;
    uint64_t class_block_rkey_;
    uint64_t heap_start_;

    // << allocation metadata >>
    one_side_info m_one_side_info_;
    section_e current_section_;
    uint32_t current_section_index_;
    region_with_rkey current_region_;
    std::unordered_map<uint16_t, region_with_rkey> exclusive_region_;
    region_e backup_region_;
    region_e current_class_region_[16];

    // << reserved block cache>>
    rdma_mem_t ring_cache[ring_buffer_size];
    uint32_t reader, writer;
    float cache_watermark_low;
    float cache_watermark_high;
    uint64_t cache_upper_bound;
    rdma_mem_t ring_class_cache[16][class_ring_buffer_size];
    uint64_t class_cache_upper_bound[16];
    uint32_t class_reader[16], class_writer[16];

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
    
    ConnectionManager *m_rdma_conn_;
    std::vector<rdma_mem_t> m_used_mem_; /* the used mem */
    std::mutex m_mutex_;                 /* used for concurrent mem allocation */

    uint64_t time_stamp_;
};

}