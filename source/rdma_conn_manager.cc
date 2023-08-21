/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:27
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-08-14 17:32:59
 * @FilePath: /rmalloc_newbase/source/rdma_conn_manager.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#include "rdma_conn_manager.h"
#include "atomic"

namespace mralloc {

int ConnectionManager::init(const std::string ip, const std::string port,
                            uint32_t rpc_conn_num,
                            uint32_t one_sided_conn_num) {
  m_rpc_conn_queue_ = new ConnQue();
  m_one_sided_conn_queue_ = new ConnQue();
  if (rpc_conn_num > MAX_SERVER_WORKER) {
    printf(
        "max server worker is %d, rpc_conn_num is: %d, reset rpc_conn_num to "
        "%d\n",
        MAX_SERVER_WORKER, rpc_conn_num, MAX_SERVER_WORKER);
    rpc_conn_num = MAX_SERVER_WORKER;
  }

  for (uint32_t i = 0; i < rpc_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection();
    if (conn->init(ip, port)) {
      // TODO: release resources
      return -1;
    }
    m_rpc_conn_queue_->enqueue(conn);
  }

  for (uint32_t i = 0; i < one_sided_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection();
    if (conn->init(ip, port)) {
      // TODO: release resources
      return -1;
    }
    m_one_sided_conn_queue_->enqueue(conn);
  }
  return 0;
}

int ConnectionManager::register_remote_memory(uint64_t &addr, uint32_t &rkey,
                                              uint64_t size) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->register_remote_memory(addr, rkey, size);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_read(void *ptr, uint32_t size,
                                   uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = m_one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_read(ptr, size, remote_addr, rkey);
  m_one_sided_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_write(void *ptr, uint32_t size,
                                    uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = m_one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_write(ptr, size, remote_addr, rkey);
  m_one_sided_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_fetch_block(uint64_t &addr, uint32_t &rkey,
                                          uint64_t size) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_fetch_block(addr, rkey, size);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_fetch_fast_block(uint64_t &addr, uint32_t &rkey) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_fetch_fast_block(addr, rkey);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

}  // namespace kv