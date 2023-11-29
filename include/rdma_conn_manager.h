/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-10-23 15:05:13
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-14 17:41:19
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
    int remote_read(void *ptr, uint32_t size, uint64_t remote_addr,
                    uint32_t rkey);
    int remote_write(void *ptr, uint32_t size, uint64_t remote_addr,
                    uint32_t rkey);
    bool remote_CAS(uint64_t swap, uint64_t* compare, uint64_t remote_addr, 
                        uint32_t rkey);
    int remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey);
    int remote_rebind(uint64_t addr, uint32_t block_class, uint32_t &newkey);
    int remote_class_bind(uint16_t region_offset, uint16_t block_class);
    int remote_memzero(uint64_t addr, uint64_t size);
    int remote_fetch_block(uint64_t &addr, uint32_t &rkey, uint64_t size);
    int remote_fetch_block(uint64_t &addr, uint32_t &rkey);
    uint32_t get_global_rkey() {return global_rkey_;};
    RDMAConnection* fetch_connector() {
        return m_rpc_conn_queue_->dequeue();
    };

    void return_connector(RDMAConnection* connector) {
        m_rpc_conn_queue_->enqueue(connector);
    };

    bool update_section(region_e region, alloc_advise advise, alloc_advise compare);
    bool find_section(section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) ;

    bool fetch_large_region(section_e &alloc_section, uint32_t section_offset, uint64_t region_num, uint64_t &addr) ;
    bool fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region) ;
    bool try_add_fast_region(uint32_t section_offset, uint32_t block_class, region_e &alloc_region);
    bool set_region_exclusive(region_e &alloc_region);
    bool set_region_empty(region_e &alloc_region);

    bool init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive);
    bool fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive) ;
    bool fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive) ;

    bool fetch_exclusive_region_rkey(region_e &alloc_region, uint32_t* rkey_list);
    bool free_region_block(uint64_t addr, bool is_exclusive) ;

  // << one side alloc API >>
//   int remote_fetch_block_one_sided(uint64_t &addr, uint32_t &rkey);

 private:
  
  ConnQue *m_rpc_conn_queue_;
  ConnQue *m_one_sided_conn_queue_;

  one_side_info m_one_side_info_;
  uint32_t global_rkey_;

};

};  // namespace kv