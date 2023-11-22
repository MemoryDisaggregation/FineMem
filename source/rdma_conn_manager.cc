/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:27
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-14 15:11:39
 * @FilePath: /rmalloc_newbase/source/rdma_conn_manager.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#include "rdma_conn_manager.h"
#include "atomic"
#include "msg.h"

namespace mralloc {

int ConnectionManager::init(const std::string ip, const std::string port,
                            uint32_t rpc_conn_num,
                            uint32_t one_sided_conn_num) {
  m_rpc_conn_queue_ = new ConnQue();
  m_one_sided_conn_queue_ = new ConnQue();
  if (rpc_conn_num > MAX_SERVER_WORKER * MAX_SERVER_CLIENT) {
    printf(
        "max server worker is %d, rpc_conn_num is: %d, reset rpc_conn_num to "
        "%d\n",
        MAX_SERVER_WORKER, rpc_conn_num, MAX_SERVER_WORKER);
    rpc_conn_num = MAX_SERVER_WORKER;
  }

  for (uint32_t i = 0; i < rpc_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection();
    if (conn->init(ip, port, CONN_RPC)) {
      // TODO: release resources
      return -1;
    }
    m_one_side_info_ = conn->get_one_side_info();
    global_rkey_ = conn->get_global_rkey();
    m_rpc_conn_queue_->enqueue(conn);
  }

  for (uint32_t i = 0; i < one_sided_conn_num; i++) {
    RDMAConnection *conn = new RDMAConnection();
    if (conn->init(ip, port, CONN_ONESIDE)) {
      // TODO: release resources
      return -1;
    }
    // conn->malloc_hint((uint64_t)0x28000000, i);
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

bool ConnectionManager::remote_CAS(uint64_t swap, uint64_t* compare, uint64_t remote_addr, uint32_t rkey) {
  RDMAConnection *conn = m_one_sided_conn_queue_->dequeue();
  assert(conn != nullptr);
  bool ret = conn->remote_CAS(swap, compare, remote_addr, rkey);
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

int ConnectionManager::remote_fetch_block(uint64_t &addr, uint32_t &rkey) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_fetch_block(addr, rkey);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

int ConnectionManager::remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey){
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_mw(addr, rkey, size, newkey);
  m_rpc_conn_queue_->enqueue(conn);
  return ret;
}

// int ConnectionManager::remote_fetch_block_one_sided(uint64_t &addr, uint32_t &rkey) {
//   RDMAConnection *conn = m_one_sided_conn_queue_->dequeue();
//   assert(conn != nullptr);
// //   int ret = conn->remote_fetch_block_one_sided(addr, rkey);
//   int ret = conn->remote_fetch_block_one_sided(addr, rkey);
//   m_one_sided_conn_queue_->enqueue(conn);
//   return ret;
// } 

}  // namespace kv