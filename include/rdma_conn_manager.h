/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-10-23 15:05:13
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-05 17:11:51
 * @FilePath: /rmalloc_newbase/include/rdma_conn_manager.h
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <bits/stdint-uintn.h>
#include <mutex>
#include <queue>
#include <thread>
#include "rdma_conn.h"

namespace mralloc {

/* The RDMA connection queue */
class ConnQue {
 public:
  ConnQue() {}

  void enqueue(RDMAConnection *conn) {
    std::unique_lock<std::mutex> lock(m_mutex_);
    m_queue_.push(conn);
  };

  RDMAConnection *dequeue() {
  retry:
    std::unique_lock<std::mutex> lock(m_mutex_);
    while (m_queue_.empty()) {
      lock.unlock();
      std::this_thread::yield();
      goto retry;
    }
    RDMAConnection *conn = m_queue_.front();
    m_queue_.pop();
    return conn;
  }

 private:
  std::queue<RDMAConnection *> m_queue_;
  std::mutex m_mutex_;
};

/* The RDMA connection manager */
class ConnectionManager {
 public:
    ConnectionManager() {}

    ~ConnectionManager() {
        // TODO: release resources;
    }

    int init(const std::string ip, const std::string port, uint32_t rpc_conn_num,
            uint32_t one_sided_conn_num);
    one_side_info get_one_side_info() {return m_one_side_info_;};
    int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
    int unregister_remote_memory(uint64_t addr);
    int remote_read(void *ptr, uint32_t size, uint64_t remote_addr,
                    uint32_t rkey);
    int remote_write(void *ptr, uint32_t size, uint64_t remote_addr,
                    uint32_t rkey);
    bool remote_CAS(uint64_t swap, uint64_t* compare, uint64_t remote_addr, 
                        uint32_t rkey);
    int remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey);
    int remote_rebind(uint64_t addr, uint32_t block_class, uint32_t &newkey);
    int remote_rebind_batch(uint64_t *addr, uint32_t *newkey);
    int remote_class_bind(uint32_t region_offset, uint16_t block_class);
    int remote_memzero(uint64_t addr, uint64_t size);
    int remote_fetch_block(uint64_t &addr, uint32_t &rkey, uint64_t size);
    int remote_fetch_block(uint64_t &addr, uint32_t &rkey);
    int remote_free_block(uint64_t addr);
    int remote_print_alloc_info();
    uint32_t get_global_rkey() {return global_rkey_;};
    RDMAConnection* fetch_connector() {
        return m_rpc_conn_queue_->dequeue();
    };

    void return_connector(RDMAConnection* connector) {
        m_rpc_conn_queue_->enqueue(connector);
    };

    bool update_section(uint32_t region_index, alloc_advise advise, alloc_advise compare);
    bool find_section(uint16_t block_class, section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) ;

    bool fetch_large_region(section_e &alloc_section, uint32_t section_offset, uint64_t region_num, uint64_t &addr) ;
    bool fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region, uint32_t &region_index) ;
    bool try_add_section_class(uint32_t section_offset, uint32_t block_class, uint32_t region_index);
    bool set_region_exclusive(region_e &alloc_region, uint32_t region_index);
    bool set_region_empty(region_e &alloc_region, uint32_t region_index);

    bool init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive, uint32_t region_index);
    int fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) ;
    int fetch_region_batch(region_e &alloc_region, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index);
    bool fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) ;
    int fetch_region_class_batch(region_e &alloc_region, uint32_t block_class, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index);

    int fetch_block(uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) ;
    bool fetch_block(uint16_t block_class, uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) ;
    bool free_block(uint64_t addr) ;
    bool free_block(uint16_t block_class, uint64_t addr) ;
    int free_region_batch(uint32_t region_offset, uint32_t free_bitmap, bool is_exclusive);

    bool fetch_exclusive_region_rkey(uint32_t region_index, uint32_t* rkey_list);
    bool fetch_class_region_rkey(uint32_t region_index, uint32_t* rkey_list);
    int free_region_block(uint64_t addr, bool is_exclusive) ;

    inline uint32_t get_addr_region_index(uint64_t addr) {return (addr-heap_start_) / region_size_;};
    inline uint32_t get_addr_region_offset(uint64_t addr) {return (addr-heap_start_) % region_size_ / block_size_;};
    inline uint32_t get_section_class_index(uint32_t section_offset, uint32_t block_class) {return section_offset*block_class_num + block_class;};
    inline uint64_t get_section_region_addr(uint32_t section_offset, uint32_t region_offset) {return heap_start_ + section_offset*section_size_ + region_offset * region_size_ ;};
    inline uint64_t get_region_addr(uint32_t region_index) {return heap_start_ + region_index * region_size_;};
    inline uint64_t get_region_block_addr(uint32_t region_index, uint32_t block_offset) {return heap_start_ + region_index * region_size_ + block_offset * block_size_;} ;
    inline uint32_t get_region_block_rkey(uint32_t region_index, uint32_t block_offset) {
        uint32_t rkey; uint32_t rkey_new = -1;
        // do{
            remote_read(&rkey, sizeof(rkey), block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);
        // }while(rkey == -1);
        // if(rkey == -1){
            // remote_rebind(get_region_block_addr(region_index, block_offset), 0, rkey);
        // }
        // remote_write(&rkey_new, sizeof(uint32_t), block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);

        // rkey_CAS = rkey;
        // if(!remote_CAS((uint32_t)-1, &rkey_CAS, block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_)){
        //     printf("rkey cas failed!\n");
        //     return 0;
        // }
        return rkey;
    };
    inline uint32_t get_region_class_block_rkey(uint32_t region_index, uint32_t block_offset) {
        uint32_t rkey;
        remote_read(&rkey, sizeof(rkey), class_block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);
        return rkey;
    };

    inline uint32_t rebind_region_block_rkey(uint32_t region_index, uint32_t block_offset) {
        uint32_t rkey; uint32_t rkey_new = -1;
        remote_read(&rkey, sizeof(rkey), backup_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);
        if(rkey == (uint32_t)-1 || rkey == 0){
            return 0;
        }
        remote_write(&rkey_new, sizeof(uint32_t), backup_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);

        // rkey_CAS = rkey;
        // if(!remote_CAS((uint32_t)-1, &rkey_CAS, block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_)){
        //     printf("rkey cas failed!\n");
        //     return 0;
        // }
        return rkey;
    };
    
    inline uint32_t get_block_num() {return block_num_;};

 private:
    
    ConnQue *m_rpc_conn_queue_;
    ConnQue *m_one_sided_conn_queue_;

    one_side_info m_one_side_info_;
    uint32_t global_rkey_;

      // basic info
    uint64_t block_size_;
    uint64_t block_num_;
    uint64_t region_size_;
    uint64_t region_num_;
    uint64_t section_size_;
    uint64_t section_num_;

    // info before heap segment
    uint64_t section_header_;
    uint64_t section_class_header_;
    uint64_t region_header_;
    uint64_t block_rkey_;
    uint64_t class_block_rkey_;
    uint64_t heap_start_;
    uint64_t block_header_;
    uint64_t backup_rkey_;

};

};  // namespace kv
