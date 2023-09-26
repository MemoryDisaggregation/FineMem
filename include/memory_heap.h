/*
 * @Author: blahaj wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 16:09:32
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-25 16:59:19
 * @FilePath: /rmalloc_newbase/include/memory_heap.h
 * @Description: memory heap for rmalloc
 */
#pragma once

#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <sched.h>
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
#include "rdma_conn_manager.h"
#include "rdma_mem_pool.h"
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

/* The RDMA connection queue */
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

  LocalHeap() {

  }

  bool start(const std::string addr, const std::string port) override;

  void stop() override;

  bool alive() override;

  void run() override;

  ~LocalHeap() { destory(); }

  void fetch_cache(uint8_t nproc, uint64_t &addr, uint32_t &rkey);

  bool fetch_mem_fast(uint64_t &addr);

  bool fetch_mem_remote(uint64_t size, uint64_t &addr, uint32_t &rkey);

  // alloc 2MB memory blocks
  bool fetch_mem_fast_remote(uint64_t &addr, uint32_t &rkey);

  // alloc 2MB aligned large blocks
  bool fetch_mem_align_remote(uint64_t size, uint64_t &addr, uint32_t &rkey);

  bool mr_bind_remote(uint64_t size, uint64_t addr, uint32_t rkey, uint32_t &newkey);

 private:
  void destory(){};

  // TODO: cpu cache using shm_open;
  // std::queue<uint64_t> cpu_free_pages_[nprocs];
  // std::unordered_map<uint64_t, pid_t>* cpu_allocated_pages_[nprocs];
  cpu_cache* cpu_cache_;
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

  ~RemoteHeap(){};

  bool start(const std::string addr, const std::string port) override;
  void stop() override;
  bool alive() override;
  bool fetch_mem_local(uint64_t &addr, uint64_t size, uint32_t &lkey, uint32_t &rkey);
  bool fetch_mem_fast_local(uint64_t &addr, uint32_t &lkey, uint32_t &rkey);
  bool fetch_mem_fast_remote(uint64_t &addr, uint32_t &rkey);

  void print_alloc_info();

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
  RPC_Fusee* rpc_fusee_;
};

}  // namespace kv