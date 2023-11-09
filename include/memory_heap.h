/*
 * @Author: blahaj wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:09:32
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-07 10:15:51
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

#define RDMA_ALLOCATE_SIZE (1 << 26ul)

// const uint8_t nprocs = get_nprocs();

 
namespace mralloc {

/*
struct cpu_cache {
  uint64_t read_p[nproc];
  uint64_t write_p[nproc];
  uint64_t cache[nproc][max_item];
};
*/

/* The memory window queue, not used now */
class MWQueue {
 public:
  MWQueue(ibv_pd *pd) :pd_(pd) {
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

class MemHeap {
 public:
    MemHeap(){}

    virtual bool start(const std::string addr, const std::string port) { return true; }

    virtual void stop() {}

    virtual bool alive() { return true; }

    virtual void run() {}

    void set_global_rkey(uint32_t rkey) {
      global_rkey_ = rkey;
    }

    uint32_t get_global_rkey() {
      return global_rkey_;
    }

    ~MemHeap() {}
 
 protected:
    FreeBlockManager *free_queue_manager;
    uint8_t running;
    uint32_t global_rkey_;

};

void * run_heap(void* arg) ;


class LocalHeap: public MemHeap {
 public:
  typedef struct {
    uint64_t addr;
    uint32_t rkey;
  } rdma_mem_t;

  LocalHeap(bool heap_enabled, bool cache_enabled, bool one_side_enabled): heap_enabled_(heap_enabled), cpu_cache_enabled_(cache_enabled), one_side_enabled_(one_side_enabled) {
    if(cpu_cache_enabled_)  assert(heap_enabled_);
  }

  bool start(const std::string addr, const std::string port) override;

  void stop() override;

  bool alive() override;

  void run() override;

  ~LocalHeap() { destory(); }

  // << one-sided block fetch >>
  // bool update_mem_metadata();
  // bool update_rkey_metadata();
  bool fetch_mem_one_sided(uint64_t &addr, uint32_t &rkey);
  // bool fetch_rkey_list_one_sided(uint64_t addr, uint32_t* rkey_list);

  // << RPC block fetch >>
  bool fetch_mem_fast_remote(uint64_t &addr, uint32_t &rkey);
  bool fetch_mem_align_remote(uint64_t size, uint64_t &addr, uint32_t &rkey);

  // << RPC block fetch & local heap/cache fetch >>
  void fetch_cache(uint8_t nproc, uint64_t &addr, uint32_t &rkey);
  bool fetch_mem_fast(uint64_t &addr, uint32_t &rkey);
  bool fetch_mem_remote(uint64_t size, uint64_t &addr, uint32_t &rkey);

  // UNUSED
  bool mr_bind_remote(uint64_t size, uint64_t addr, uint32_t rkey, uint32_t &newkey);

  ConnectionManager* get_conn(){return m_rdma_conn_;};

 private:
  void destory(){};

  // << function enabled >>
  bool heap_enabled_;
  bool cpu_cache_enabled_;
  bool one_side_enabled_;

  // << cpu cache >>
  cpu_cache* cpu_cache_;
  std::atomic<uint8_t> heap_worker_id_;
  uint8_t heap_worker_num_;

  // << one-side metadata >>
  one_side_info m_one_side_info_;
  block_header_e* header_list;
  uint32_t* rkey_list;
  uint64_t last_alloc_;
  
  ConnectionManager *m_rdma_conn_;
  uint32_t global_rkey_;
  std::vector<rdma_mem_t> m_used_mem_; /* the used mem */
  std::mutex m_mutex_;                 /* used for concurrent mem allocation */


};

class RemoteHeap : public MemHeap {
 public:
  struct WorkerInfo {
    CmdMsgBlock *cmd_msg;
    CmdMsgRespBlock *cmd_resp_msg;
    struct ibv_mr *msg_mr;
    struct ibv_mr *resp_mr;
    rdma_cm_id *cm_id;
    struct ibv_cq *cq;
  };
  RemoteHeap(bool one_sided_enabled): one_sided_enabled_(one_sided_enabled) {};
  ~RemoteHeap(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;
  bool fetch_mem_local(uint64_t &addr, uint64_t size, uint32_t &lkey, uint32_t &rkey);
  bool fetch_mem_local(uint64_t start_addr, uint64_t &addr, uint64_t size, uint32_t &lkey, uint32_t &rkey);
  bool fetch_mem_fast_local(uint64_t &addr, uint32_t &lkey, uint32_t &rkey);
  bool fetch_mem_fast_remote(uint64_t &addr, uint32_t &rkey);

  void print_alloc_info();

  bool init_mw(ibv_qp* qp, ibv_cq *cq);
  bool bind_mw(ibv_mw* mw, uint64_t addr, uint64_t size, ibv_qp* qp, ibv_cq *cq);
  bool bind_mw_type2(ibv_mw* mw, uint64_t addr, uint64_t size, ibv_qp* qp, ibv_cq *cq);
  bool unbind_mw_type2(ibv_mw* mw, uint64_t addr, uint64_t size, ibv_qp* qp, ibv_cq *cq);

 private:

  bool init_memory_heap(uint64_t size);

  void handle_connection();

  int create_connection(struct rdma_cm_id *cm_id, uint8_t connect_type);

  struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

  int remote_write(WorkerInfo *work_info, uint64_t local_addr, uint32_t lkey,
                   uint32_t length, uint64_t remote_addr, uint32_t rkey);

  int allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                   uint64_t size);

  void worker(WorkerInfo *work_info, uint32_t num);

  // << function enabled >>
  bool one_sided_enabled_;

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
  MWQueue* mw_queue_;
  ibv_mw **m_mw_handler;
  RPC_Fusee* rpc_fusee_;
  uint64_t heap_total_size_;
  uint64_t heap_start_addr_;
  bool mw_binded;
  ibv_cq* mw_cq;
  ibv_qp* mw_qp;

  ibv_mw** block_mw;
  ibv_mw** base_mw;

};

}  // namespace kv