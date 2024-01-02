/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:27
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-05 17:14:37
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
    block_size_ = m_one_side_info_.block_size_;
    block_num_ = m_one_side_info_.block_num_;
    region_size_ = block_size_ * block_per_region;
    region_num_ = block_num_ / block_per_region;
    section_size_ = region_size_ * region_per_section;
    section_num_ = region_num_ / region_per_section;

    section_header_ = m_one_side_info_.section_header_;
    section_class_header_ = (uint64_t)((section_e*)section_header_ + section_num_);
    region_header_ = (uint64_t)((section_class_e*)section_class_header_ + block_class_num*section_num_);
    block_rkey_ = (uint64_t)((region_e*)region_header_ + region_num_);
    class_block_rkey_ = (uint64_t)((uint32_t*)block_rkey_ + block_num_);
    heap_start_ = m_one_side_info_.heap_start_;
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

int ConnectionManager::unregister_remote_memory(uint64_t addr) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->unregister_remote_memory(addr);
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

int ConnectionManager::fetch_region_batch(region_e &alloc_region, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->fetch_region_batch(alloc_region, addr, num, is_exclusive, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

int ConnectionManager::remote_free_block(uint64_t addr) {
  RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
  assert(conn != nullptr);
  int ret = conn->remote_free_block(addr);
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

bool ConnectionManager::update_section(uint32_t region_index, alloc_advise advise, alloc_advise compare) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->update_section(region_index, advise, compare);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}
bool ConnectionManager::find_section(uint16_t block_class, section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->find_section(block_class, alloc_section, section_offset, advise);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

bool ConnectionManager::fetch_large_region(section_e &alloc_section, uint32_t section_offset, uint64_t region_num, uint64_t &addr) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_large_region(alloc_section, section_offset, region_num, addr);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}
bool ConnectionManager::fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region, uint32_t &region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_region(alloc_section, section_offset, block_class, shared, alloc_region, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}
bool ConnectionManager::try_add_section_class(uint32_t section_offset, uint32_t block_class, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->try_add_section_class(section_offset, block_class, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;    
}
bool ConnectionManager::set_region_exclusive(region_e &alloc_region, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->set_region_exclusive(alloc_region, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;      
}
bool ConnectionManager::set_region_empty(region_e &alloc_region, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->set_region_empty(alloc_region, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;      
}

bool ConnectionManager::init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->init_region_class(alloc_region, block_class, is_exclusive, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;        
}
int ConnectionManager::fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->fetch_region_block(alloc_region, addr, rkey, is_exclusive, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;  
}

bool ConnectionManager::fetch_block(uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_block(block_hint, addr, rkey);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;  
}

bool ConnectionManager::free_block(uint64_t addr) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->free_block(addr);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;  
}

bool ConnectionManager::fetch_block(uint16_t block_class, uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_block(block_class, block_hint, addr, rkey);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;  
}

bool ConnectionManager::free_block(uint16_t block_class, uint64_t addr) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->free_block(block_class, addr);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;  
}

bool ConnectionManager::fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_region_class_block(alloc_region, block_class, addr, rkey, is_exclusive, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;      
}

int ConnectionManager::fetch_region_class_batch(region_e &alloc_region, uint32_t block_class, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->fetch_region_class_batch(alloc_region, block_class, addr, num, is_exclusive, region_index);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;      
}

bool ConnectionManager::fetch_exclusive_region_rkey(uint32_t region_index, uint32_t* rkey_list) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_exclusive_region_rkey(region_index, rkey_list);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

bool ConnectionManager::fetch_class_region_rkey(uint32_t region_index, uint32_t* rkey_list) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    bool ret = conn->fetch_class_region_rkey(region_index, rkey_list);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

int ConnectionManager::remote_rebind(uint64_t addr, uint32_t block_class, uint32_t &newkey) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->remote_rebind(addr, block_class, newkey);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

int ConnectionManager::remote_class_bind(uint32_t region_offset, uint16_t block_class) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->remote_class_bind(region_offset, block_class);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

int ConnectionManager::remote_memzero(uint64_t addr, uint64_t size) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->remote_memzero(addr, size);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

int ConnectionManager::remote_print_alloc_info() {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->remote_print_alloc_info();
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}


int ConnectionManager::free_region_block(uint64_t addr, bool is_exclusive) {
    RDMAConnection *conn = m_rpc_conn_queue_->dequeue();
    assert(conn != nullptr);
    int ret = conn->free_region_block(addr, is_exclusive);
    m_rpc_conn_queue_->enqueue(conn);
    return ret;
}

}  // namespace kv
