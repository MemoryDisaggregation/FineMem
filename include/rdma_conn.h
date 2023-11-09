/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:26
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-10-23 15:38:05
 * @FilePath: /rmalloc_newbase/include/rdma_conn.h
 * @Description: RDMA Connection functions, with RDMA read/write and fetch block, used by both LocalHeap and RemoteHeap
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <map>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include "msg.h"
#include "free_block_manager.h"

namespace mralloc {

#define RESOLVE_TIMEOUT_MS 5000

struct one_side_info {
  uint64_t m_header_addr_;
  uint64_t m_rkey_addr_;
  uint64_t m_block_addr_;
  uint64_t m_block_num;
  uint64_t m_base_size;
  uint64_t m_fast_size;
};

/* RDMA connection */
class RDMAConnection {
 public:
  int init_async();
  int connect_async(const std::string ip, const std::string port, uint8_t access_type);
  int init(const std::string ip, const std::string port, uint8_t access_type);
  int init(const std::string ip, const std::string port, ibv_context* ctx, ibv_pd* pd, ibv_cq* cq, uint8_t access_type);
  one_side_info get_one_side_info() {return m_one_side_info_;};
  int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int remote_read(void *ptr, uint64_t size, uint64_t remote_addr,
                  uint32_t rkey);
  int remote_write(void *ptr, uint64_t size, uint64_t remote_addr,
                   uint32_t rkey);
  uint64_t remote_CAS(uint64_t swap, uint64_t compare, uint64_t remote_addr, 
                    uint32_t rkey);
  int remote_fetch_block(uint64_t &addr, uint32_t &rkey, uint64_t size);
  int remote_fetch_fast_block(uint64_t &addr, uint32_t &rkey);
  int remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey);
  int remote_fusee_alloc(uint64_t &addr, uint32_t &rkey);
  uint32_t get_rkey() {return m_fusee_rkey;};
  uint32_t get_global_rkey() {return global_rkey_;};
  ibv_qp* get_qp() {return m_cm_id_->qp;};
  ibv_cq* get_cq() {return m_cq_;};
  ibv_pd* get_pd() {return m_pd_;};
  ibv_context* get_ctx() {return m_cm_id_->verbs;};

  // << one-sided fetch API >>
  bool fetch_mem_one_sided(uint64_t &addr, uint32_t &rkey);
  bool malloc_hint(uint64_t start, uint64_t idx);

 private:
  inline uint64_t find_free_index_from_bitmap(uint64_t bitmap) {
    return __builtin_ctzll(~bitmap);
  }
  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  // << one-sided support functions >>
  bool update_mem_metadata(uint64_t index);
  bool update_mem_bitmap(uint64_t index);
  bool update_rkey_metadata();
  bool fetch_rkey_list_one_sided(uint64_t addr, uint32_t* rkey_list);

  // << one-sided read/write >>
  int rdma_remote_read(uint64_t local_addr, uint32_t lkey, uint64_t length,
                       uint64_t remote_addr, uint32_t rkey);

  int rdma_remote_write(uint64_t local_addr, uint32_t lkey, uint64_t length,
                        uint64_t remote_addr, uint32_t rkey);

  struct rdma_event_channel *m_cm_channel_;
  struct ibv_pd *m_pd_;
  struct ibv_cq *m_cq_;
  struct rdma_cm_id *m_cm_id_;
  uint64_t m_server_cmd_msg_;
  uint32_t m_server_cmd_rkey_;
  uint32_t m_fusee_rkey;
  uint32_t m_remote_size_;
  struct CmdMsgBlock *m_cmd_msg_;
  struct CmdMsgRespBlock *m_cmd_resp_;
  struct ibv_mr *m_msg_mr_;
  struct ibv_mr *m_resp_mr_;
  char *m_reg_buf_;
  struct ibv_mr *m_reg_buf_mr_;
  uint8_t conn_id_;

  // << one-sided support >>
  one_side_info m_one_side_info_;
  uint32_t global_rkey_;
  large_block_lockless block_;
  uint32_t* rkey_list;
  uint64_t last_alloc_;
  uint64_t user_start_;
  int total_old_= 0;

};

static bool* full_bitmap;


}  // namespace kv