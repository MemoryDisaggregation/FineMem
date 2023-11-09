/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-10-23 15:05:13
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-10-23 15:38:13
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
  uint64_t remote_CAS(uint64_t swap, uint64_t compare, uint64_t remote_addr, 
                    uint32_t rkey);
  int remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey);
  int remote_fetch_block(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int remote_fetch_fast_block(uint64_t &addr, uint32_t &rkey);
  uint32_t get_global_rkey() {return global_rkey_;};
  
  // << one side alloc API >>
  bool fetch_mem_one_sided(uint64_t &addr, uint32_t &rkey);

 private:
  
  ConnQue *m_rpc_conn_queue_;
  ConnQue *m_one_sided_conn_queue_;

  one_side_info m_one_side_info_;
  uint32_t global_rkey_;

};

};  // namespace kv