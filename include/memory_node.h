/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-11-21 17:26:29
 * @LastEditors: blahaj wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-06 22:03:07
 * @FilePath: /rmalloc_newbase/include/memory_node.h
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
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

class MWPool {
 public:
  MWPool(ibv_pd *pd) :pd_(pd) {
    for(int i=0; i<10; i++){
        ibv_mw* new_mw_ = ibv_alloc_mw(pd_, IBV_MW_TYPE_1);
        // printf("generate rkey:%x\n", new_mw_->rkey);
        mw_queue_.push(new_mw_);
    }
  }

  void enqueue(ibv_mw *conn) {
    std::unique_lock<std::mutex> lock(mw_mutex_);
    mw_queue_.push(conn);
  };

  ibv_mw *dequeue() {
    std::unique_lock<std::mutex> lock(mw_mutex_);
    if(mw_queue_.empty()) {
      for(int i=0; i<10; i++){
        ibv_mw* new_mw_ = ibv_alloc_mw(pd_, IBV_MW_TYPE_1);
        mw_queue_.push(new_mw_);
      }
    }
    ibv_mw *mw = mw_queue_.front();
    mw_queue_.pop();
    return mw;
  }

 private:
  std::queue<ibv_mw*> mw_queue_;
  std::mutex mw_mutex_;
  ibv_pd *pd_;
};

class MemoryNode {
public:
    struct WorkerInfo {
        CmdMsgBlock *cmd_msg;
        CmdMsgRespBlock *cmd_resp_msg;
        struct ibv_mr *msg_mr;
        struct ibv_mr *resp_mr;
        rdma_cm_id *cm_id;
        struct ibv_cq *cq;
    };    
    MemoryNode(bool one_sided_enabled): one_sided_enabled_(one_sided_enabled) {
        ring_cache = new ring_buffer_atomic<mr_rdma_addr>(8192, ring_cache_content, mr_rdma_addr(-1, -1), &reader, &writer);
        ring_cache -> clear();
    };
    ~MemoryNode(){};

    bool start(const std::string addr, const std::string port, const std::string device);
    void stop();
    bool alive();

    bool new_cache_section(uint32_t block_class);
    bool new_cache_region(uint32_t block_class);
    bool fill_cache_block(uint32_t block_class);

    bool fetch_mem_block(uint64_t &addr, uint32_t &rkey);
    bool fetch_mem_class_block(uint64_t &addr, uint32_t &rkey);
    bool free_mem_block(uint64_t addr);
    
    // deprecated functions
    // bool fetch_mem_local(uint64_t &addr, uint64_t size, uint32_t &lkey, uint32_t &rkey);
    // bool fetch_mem_local(uint64_t start_addr, uint64_t &addr, uint64_t size, uint32_t &lkey, uint32_t &rkey);
    // bool fetch_mem_block_local(uint64_t &addr, uint32_t &lkey, uint32_t &rkey);
    // bool fetch_mem_block_remote(uint64_t &addr, uint32_t &rkey);

    void print_alloc_info();

    bool init_mw(ibv_qp* qp, ibv_cq *cq);
    bool init_class_mw(uint16_t region_offset, uint16_t block_class, ibv_qp* qp, ibv_cq *cq);
    bool bind_mw(ibv_mw* mw, uint64_t addr, uint64_t size, ibv_qp* qp, ibv_cq *cq);
    bool bind_mw_type2(ibv_mw* mw, uint64_t addr, uint64_t size, ibv_qp* qp, ibv_cq *cq);
    bool unbind_mw_type2(ibv_mw* mw, uint64_t addr, uint64_t size, ibv_qp* qp, ibv_cq *cq);

    void set_global_rkey(uint32_t rkey) {
      global_rkey_ = rkey;
    }

    uint32_t get_global_rkey() {
      return global_rkey_;
    }

private:

    bool init_memory_heap(uint64_t size);

    void handle_connection();

    int create_connection(struct rdma_cm_id *cm_id, uint8_t connect_type);

    struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

    int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey,
                    uint32_t length, uint64_t remote_addr, uint32_t rkey);

    int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                    uint64_t size);

    int deallocate_and_unregister_memory(uint64_t addr);

    void worker(WorkerInfo *work_info, uint32_t num);

    // << allocation metadata >>
    section_e current_section_;
    uint32_t current_section_index_;
    region_e current_region_;
    region_e current_class_region_[16];

    // << reserved block cache>>
    ring_buffer_atomic<mr_rdma_addr>* ring_cache;
    mr_rdma_addr ring_cache_content[8192];
    std::atomic<uint32_t> reader, writer;
    uint64_t simple_cache_addr[32];
    uint32_t simple_cache_rkey[32];
    uint64_t simple_cache_watermark;
    uint64_t simple_class_cache_addr[16][4];
    uint32_t simple_class_cache_rkey[16][4];
    uint64_t simple_class_cache_watermark[16];

    // << function enabled >>
    bool one_sided_enabled_;
    ibv_qp* one_side_qp_;
    ibv_cq* one_side_cq_;

    struct ibv_mr *global_mr_;
    struct rdma_event_channel *m_cm_channel_;
    struct rdma_cm_id *m_listen_id_;
    struct ibv_pd *m_pd_;
    struct ibv_context *m_context_;
    bool m_stop_;
    std::thread *m_conn_handler_;
    WorkerInfo **m_worker_info_;
    uint32_t m_worker_num_;
    std::thread **m_worker_threads_;
    MWPool* mw_queue_;
    std::unordered_map<uint64_t, ibv_mr*> mr_recorder;
    ibv_mw **m_mw_handler;
    RPC_Fusee* rpc_fusee_;
    uint64_t heap_total_size_;
    uint64_t heap_start_addr_;
    bool mw_binded;
    ibv_cq* mw_cq;
    ibv_qp* mw_qp;

    // << memory window? >>
    ibv_mw** block_mw;
    ibv_mw** block_class_mw;
    ServerBlockManager *server_block_manager_;
    uint8_t running;
    uint32_t global_rkey_;


};
}