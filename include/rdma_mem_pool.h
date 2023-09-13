#pragma once

#include <bits/stdint-uintn.h>
#include <sched.h>
#include <queue>
#include <unordered_map>
#include <sys/sysinfo.h>
#include "rdma_conn_manager.h"

#define RDMA_ALLOCATE_SIZE (1 << 26ul)

// const uint8_t nprocs = get_nprocs();
// const uint8_t nprocs = 2;

namespace mralloc {
class RDMAMemPool {
 public:
  typedef struct {
    uint64_t addr;
    uint32_t rkey;
  } rdma_mem_t;

  RDMAMemPool(ConnectionManager *conn_manager)
      : m_rdma_conn_(conn_manager), m_current_mem_(0), m_rkey_(0), m_pos_(0) {}

  ~RDMAMemPool() { destory(); }

  int get_mem(uint64_t size, uint64_t &addr, uint32_t &rkey);

  // alloc 2MB memory blocks
  int get_mem_fast_2MB(uint64_t &addr, uint32_t &rkey);

  // alloc 2MB aligned large blocks
  int get_mem_align(uint64_t size, uint64_t &addr, uint32_t &rkey);

 private:
  void destory();

  // TODO: cpu cache using shm_open;
  // std::queue<uint64_t> cpu_free_pages_[nprocs];
  // std::unordered_map<uint64_t, pid_t>* cpu_allocated_pages_[nprocs];

  uint64_t m_current_mem_; /* current mem used for local allocation */
  uint32_t m_rkey_;        /* rdma remote key */
  uint64_t m_pos_;         /* the position used for allocation */
  std::vector<rdma_mem_t> m_used_mem_; /* the used mem */
  ConnectionManager *m_rdma_conn_;     /* rdma connection manager */
  std::mutex m_mutex_;                 /* used for concurrent mem allocation */
};
}  // namespace kv