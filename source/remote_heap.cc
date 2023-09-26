/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:27
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-25 20:24:04
 * @FilePath: /rmalloc_newbase/source/remote_heap.cc
 * @Description: A memory heap at remote memory server, control all remote memory on it, and provide coarse-grained memory allocation
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#include <cstdio>
#include <cstdlib>
#include "memory_heap.h"
#include "msg.h"
#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <linux/mman.h>

#define MEM_ALIGN_SIZE 4096

#define REMOTE_MEM_SIZE 1024*1024*64

#define INIT_MEM_SIZE ((uint64_t)1 << 33ul)

#define SERVER_BASE_ADDR 0x10000000

namespace mralloc {

void RemoteHeap::print_alloc_info() {
  free_queue_manager->print_state();
}

/**
 * @description: start remote engine service
 * @param {string} addr   empty string for RemoteHeap as server
 * @param {string} port   the port the server listened
 * @return {bool} true for success
 */
bool RemoteHeap::start(const std::string addr, const std::string port) {

  m_stop_ = false;

  m_worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER];
  m_worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    m_worker_info_[i] = nullptr;
    m_worker_threads_[i] = nullptr;
  }
  m_worker_num_ = 0;

  // get rdma device, alloc protect domain and init memory heap

  struct ibv_context **ibv_ctxs;
  int nr_devices_;
  ibv_ctxs = rdma_get_devices(&nr_devices_);
  if (!ibv_ctxs) {
    perror("get device list fail");
    return false;
  }

  m_context_ = ibv_ctxs[1];
  m_pd_ = ibv_alloc_pd(m_context_);
  if (!m_pd_) {
    perror("ibv_alloc_pd fail");
    return false;
  }

  mw_queue_ = new MWQueue(m_pd_);
  if (!mw_queue_) {
    perror("memeory window init fail");
    return false;
  }

  if(!init_memory_heap(INIT_MEM_SIZE)) {
    perror("init memory heap fail");
    return false;
  }

  // create connection manager and listen

  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return false;
  }

  if (rdma_create_id(m_cm_channel_, &m_listen_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return false;
  }

  struct sockaddr_in sin;
  sin.sin_family = AF_INET;
  sin.sin_port = htons(stoi(port));
  sin.sin_addr.s_addr = INADDR_ANY;

  if (rdma_bind_addr(m_listen_id_, (struct sockaddr *)&sin)) {
    perror("rdma_bind_addr fail");
    return false;
  }

  if (rdma_listen(m_listen_id_, 1)) {
    perror("rdma_listen fail");
    return false;
  }

  m_conn_handler_ = new std::thread(&RemoteHeap::handle_connection, this);

  // optional init 
  if(fusee_enable){
    uint64_t fusee_addr; uint32_t fusee_lkey, fusee_rkey;
    fetch_mem_local(fusee_addr, META_AREA_LEN + HASH_AREA_LEN, fusee_lkey, fusee_rkey);
    rpc_fusee_ = new RPC_Fusee(fusee_addr, fusee_addr + META_AREA_LEN, fusee_rkey);
  }

  // wait for all threads exit
  // m_conn_handler_->join();
  // for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
  //   if (m_worker_threads_[i] != nullptr) {
  //     m_worker_threads_[i]->join();
  //   }
  // }
  // getchar();
  return true;

}

/**
 * @description: init memory heap, malloc a huge memory region and register it, then init free queue manager
 * @param {uint64_t} size: memory heap size
 * @return {bool} true for success
 */
bool RemoteHeap::init_memory_heap(uint64_t size) {
  free_queue_manager = new FreeQueueManager(REMOTE_MEM_SIZE);
  void* init_addr = mmap((void*)(SERVER_BASE_ADDR) , size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
  printf("init_addr: %p\n", init_addr);
  if (init_addr == MAP_FAILED) {
    perror("mmap fail");
    return false;
  }

  global_mr_ = rdma_register_memory(init_addr, size);

  if(!free_queue_manager->init((uint64_t)init_addr, size)) {
    perror("init free queue manager fail");
    return false;
  }
  return true;
}

bool RemoteHeap::fetch_mem_local(uint64_t &addr, uint64_t size, uint32_t &lkey, uint32_t &rkey) {
  uint64_t mem_addr;
  if(!(mem_addr = free_queue_manager->fetch(size))) {
    perror("get mem fail");
    return false;
  }
  addr = mem_addr;
  lkey = global_mr_->lkey;
  rkey = global_mr_->rkey;
  return true;
}
/**
 * @description: fetch memory in local, provide lkey
 * @param {uint64_t} &addr: the address of memory
 * @param {uint32_t} &lkey: the lkey of memory
 * @return {bool} true for success
 */
bool RemoteHeap::fetch_mem_fast_local(uint64_t &addr, uint32_t &lkey, uint32_t &rkey) {
  uint64_t mem_addr;
  if(!(mem_addr = free_queue_manager->fetch_fast())) {
    perror("get mem fail");
    return false;
  }
  addr = mem_addr;
  lkey = global_mr_->lkey;
  rkey = global_mr_->rkey;
  return true;
}

/**
 * @description: fetch memory in local, provide rkey
 * @param {uint64_t} &addr: the address of memory
 * @param {uint32_t} &rkey: the rkey of memory
 * @return {bool} true for success
 */
bool RemoteHeap::fetch_mem_fast_remote(uint64_t &addr, uint32_t &rkey) {
  uint64_t mem_addr;
  if(!(mem_addr = free_queue_manager->fetch_fast())) {
    perror("get mem fail");
    return false;
  }
  addr = mem_addr;
  rkey = global_mr_->rkey;
  return true;
}

/**
 * @description: get engine alive state
 * @return {bool}  true for alive
 */
bool RemoteHeap::alive() {  // TODO
  return true;
}

/**
 * @description: stop local engine service
 * @return {void}
 */
void RemoteHeap::stop() {
  m_stop_ = true;
  if (m_conn_handler_ != nullptr) {
    m_conn_handler_->join();
    delete m_conn_handler_;
    m_conn_handler_ = nullptr;
  }
  for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++) {
    if (m_worker_threads_[i] != nullptr) {
      m_worker_threads_[i]->join();
      delete m_worker_threads_[i];
      m_worker_threads_[i] = nullptr;
    }
  }
  // TODO: release resources
  munmap((void*)SERVER_BASE_ADDR, INIT_MEM_SIZE);
  delete free_queue_manager;
}

void RemoteHeap::handle_connection() {
  printf("start handle_connection\n");
  struct rdma_cm_event *event;
  while (true) {
    if (m_stop_) break;
    if (rdma_get_cm_event(m_cm_channel_, &event)) {
      perror("rdma_get_cm_event fail");
      return;
    }
    // printf("recieve create: %u\n", event->event);
    
    if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST) {
      struct rdma_cm_id *cm_id = event->id;
      uint8_t type = *(uint8_t*)event->param.conn.private_data;
      rdma_ack_cm_event(event);
      create_connection(cm_id, type);
    } else if (event->event == RDMA_CM_EVENT_ESTABLISHED) {
      rdma_ack_cm_event(event);
    } else {
      rdma_ack_cm_event(event);
    }
  }
  printf("exit handle_connection\n");
}

int RemoteHeap::create_connection(struct rdma_cm_id *cm_id, uint8_t connect_type) {

  if (!m_pd_) {
    perror("ibv_pibv_alloc_pdoll_cq fail");
    return -1;
  }

  struct ibv_comp_channel *comp_chan = ibv_create_comp_channel(m_context_);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  struct ibv_cq *cq = ibv_create_cq(m_context_, 1, NULL, comp_chan, 0);
  if (!cq) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(cq, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 1;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_inline_data = 256;
  qp_attr.sq_sig_all = 0;

  qp_attr.send_cq = cq;
  qp_attr.recv_cq = cq;
  qp_attr.qp_type = IBV_QPT_RC;

  if (rdma_create_qp(cm_id, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  struct PData rep_pdata;
  CmdMsgBlock *cmd_msg = nullptr;
  CmdMsgRespBlock *cmd_resp = nullptr;
  struct ibv_mr *msg_mr = nullptr;
  struct ibv_mr *resp_mr = nullptr;
  cmd_msg = new CmdMsgBlock();
  memset(cmd_msg, 0, sizeof(CmdMsgBlock));
  msg_mr = rdma_register_memory((void *)cmd_msg, sizeof(CmdMsgBlock));
  if (!msg_mr) {
    perror("ibv_reg_mr cmd_msg fail");
    return -1;
  }

  cmd_resp = new CmdMsgRespBlock();
  memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
  resp_mr = rdma_register_memory((void *)cmd_resp, sizeof(CmdMsgRespBlock));
  if (!msg_mr) {
    perror("ibv_reg_mr cmd_resp fail");
    return -1;
  }

  rep_pdata.buf_addr = (uintptr_t)cmd_msg;
  rep_pdata.buf_rkey = msg_mr->rkey;
  rep_pdata.size = sizeof(CmdMsgRespBlock);

  if(connect_type == CONN_RPC){
    int num = m_worker_num_++;
    if (m_worker_num_ <= MAX_SERVER_WORKER) {
      assert(m_worker_info_[num] == nullptr);
      m_worker_info_[num] = new WorkerInfo();
      m_worker_info_[num]->cmd_msg = cmd_msg;
      m_worker_info_[num]->cmd_resp_msg = cmd_resp;
      m_worker_info_[num]->msg_mr = msg_mr;
      m_worker_info_[num]->resp_mr = resp_mr;
      m_worker_info_[num]->cm_id = cm_id;
      m_worker_info_[num]->cq = cq;

      assert(m_worker_threads_[num] == nullptr);
      m_worker_threads_[num] =
          new std::thread(&RemoteHeap::worker, this, m_worker_info_[num], num);
    }
  }

  if (connect_type == CONN_FUSEE) {
    rep_pdata.buf_rkey = global_mr_->rkey;
  }

  struct rdma_conn_param conn_param;
  conn_param.responder_resources = 16;
  conn_param.initiator_depth = 16;
  conn_param.private_data = &rep_pdata;
  conn_param.private_data_len = sizeof(rep_pdata);

  // printf("connection created, private data: %ld, addr: %ld, key: %d\n",
  //        *((uint64_t *)rep_pdata.buf_addr), rep_pdata.buf_addr,
  //        rep_pdata.buf_rkey);

  if (rdma_accept(cm_id, &conn_param)) {
    perror("rdma_accept fail");
    return -1;
  }

  return 0;
}

struct ibv_mr *RemoteHeap::rdma_register_memory(void *ptr, uint64_t size) {
  struct ibv_mr *mr =
      ibv_reg_mr(m_pd_, ptr, size,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RemoteHeap::allocate_and_register_memory(uint64_t &addr, uint32_t &rkey,
                                               uint64_t size) {
  /* align mem */
  uint64_t total_size = size + MEM_ALIGN_SIZE;
  uint64_t mem = (uint64_t)malloc(total_size);
  addr = mem;
  if (addr % MEM_ALIGN_SIZE != 0)
    addr = addr + (MEM_ALIGN_SIZE - addr % MEM_ALIGN_SIZE);
  struct ibv_mr *mr = rdma_register_memory((void *)addr, size);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return -1;
  }
  rkey = mr->rkey;
  // printf("allocate and register memory %ld %d\n", addr, rkey);
  // TODO: save this memory info for later delete
  return 0;
}

int RemoteHeap::remote_write(WorkerInfo *work_info, uint64_t local_addr,
                               uint32_t lkey, uint32_t length,
                               uint64_t remote_addr, uint32_t rkey) {
  struct ibv_sge sge;
  sge.addr = (uintptr_t)local_addr;
  sge.length = length;
  sge.lkey = lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = IBV_WR_RDMA_WRITE;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = remote_addr;
  send_wr.wr.rdma.rkey = rkey;
  if (ibv_post_send(work_info->cm_id->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // printf("remote write %ld %d\n", remote_addr, rkey);

  auto start = TIME_NOW;
  struct ibv_wc wc;
  int ret = -1;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      perror("remote write timeout");
      return -1;
    }
    int rc = ibv_poll_cq(work_info->cq, 1, &wc);
    if (rc > 0) {
      if (IBV_WC_SUCCESS == wc.status) {
        ret = 0;
        break;
      } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
        perror("cmd_send IBV_WC_WR_FLUSH_ERR");
        break;
      } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
        perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
        break;
      } else {
        perror("cmd_send ibv_poll_cq status error");
        break;
      }
    } else if (0 == rc) {
      continue;
    } else {
      perror("ibv_poll_cq fail");
      break;
    }
  }
  return ret;
}

// RPC worker

void RemoteHeap::worker(WorkerInfo *work_info, uint32_t num) {
  printf("start worker %d\n", num);
  CmdMsgBlock *cmd_msg = work_info->cmd_msg;
  CmdMsgRespBlock *cmd_resp = work_info->cmd_resp_msg;
  struct ibv_mr *resp_mr = work_info->resp_mr;
  cmd_resp->notify = NOTIFY_WORK;
  RequestsMsg request;
  while (true) {
    if (m_stop_) break;
    if (cmd_msg->notify == NOTIFY_IDLE) continue;
    cmd_msg->notify = NOTIFY_IDLE;
    RequestsMsg *request = (RequestsMsg *)cmd_msg;
    if (request->type == MSG_REGISTER) {
      /* handle memory register requests */
      RegisterRequest *reg_req = (RegisterRequest *)request;
      // printf("receive a memory register message, size: %ld\n",
      // reg_req->size);
      RegisterResponse *resp_msg = (RegisterResponse *)cmd_resp;
      if (allocate_and_register_memory(resp_msg->addr, resp_msg->rkey,
                                       reg_req->size)) {
        resp_msg->status = RES_FAIL;
      } else {
        resp_msg->status = RES_OK;
      }
      /* write response */
      remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                   sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                   reg_req->resp_rkey);
    } else if (request->type == MSG_FETCH_FAST) {
      /* handle memory fetch requests */
      // printf("receive a memory fetch message\n");
      FetchFastResponse *resp_msg = (FetchFastResponse *)cmd_resp;
      uint64_t addr;
      uint32_t rkey;
      if (fetch_mem_fast_remote(addr, rkey)) {
        resp_msg->status = RES_OK;
        resp_msg->addr = addr;
        resp_msg->rkey = rkey;
        resp_msg->size = free_queue_manager->get_fast_size();
      } else {
        resp_msg->status = RES_FAIL;
      }
      // printf("fetch 2MB memory, addr: %ld, rkey: %d\n", resp_msg->addr,
      //        resp_msg->rkey);
      /* write response */
      remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                   sizeof(CmdMsgRespBlock), request->resp_addr,
                   request->resp_rkey);
    } else if (request->type == MSG_MW_BIND) {
      // Attension: no actual used at the critical path
      MWbindRequest *resp_req = (MWbindRequest *)request;
      MWbindResponse *resp_msg = (MWbindResponse *)cmd_resp;
      uint64_t addr = resp_req->addr; 
      uint32_t rkey = resp_req->rkey; 
      uint32_t newkey = resp_req->newkey;
      uint64_t size = resp_req->size;
      if(rkey == global_mr_->rkey){
        ibv_mw* mw_ = mw_queue_->dequeue();
        newkey = ibv_inc_rkey(mw_->rkey);
        // type 1 MW
        if(0){
          struct ibv_mw_bind_info bind_info_ = {.mr = global_mr_, 
                                          .addr = addr, 
                                          .length = size,
                                          .mw_access_flags = IBV_ACCESS_REMOTE_READ | 
                                            IBV_ACCESS_REMOTE_WRITE} ;
          struct ibv_mw_bind bind_ = {.wr_id = 0, .send_flags = IBV_SEND_SIGNALED, .bind_info = bind_info_};
          if(ibv_bind_mw(work_info->cm_id->qp, mw_, &bind_)){
            perror("ibv_post_send mw_bind fail");
            resp_msg->status = RES_FAIL;
          } else {
              while (true) {
                ibv_wc wc;
                int rc = ibv_poll_cq(work_info->cq, 1, &wc);
                if (rc > 0) {
                  if (IBV_WC_SUCCESS == wc.status) {
                    // Break out as operation completed successfully
                    // printf("Break out as operation completed successfully\n");
                    resp_msg->status = RES_OK;
                    resp_msg->addr = addr;
                    resp_msg->rkey = mw_->rkey;
                    resp_msg->size = size;
                    break;
                  } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
                    perror("cmd_send IBV_WC_WR_FLUSH_ERR");
                    resp_msg->status = RES_FAIL;
                    break;
                  } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
                    perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
                    resp_msg->status = RES_FAIL;
                    break;
                  } else {
                    perror("cmd_send ibv_poll_cq status error");
                    resp_msg->status = RES_FAIL;
                    break;
                  }
                } else if (0 == rc) {
                  continue;
                } else {
                  perror("ibv_poll_cq fail");
                  resp_msg->status = RES_FAIL;
                  break;
                }
              }
          }
        }
        // type 2 MW
        else {
          struct ibv_send_wr wr_ = {};
          struct ibv_send_wr* bad_wr_;
          // struct ibv_sge sge_ = {};
          // sge_.addr = addr;
          // sge_.lkey = global_mr_->lkey;
          // sge_.length = size;
          wr_.wr_id = 0;
          wr_.num_sge = 0;
          wr_.next = NULL;
          wr_.opcode = IBV_WR_BIND_MW;
          wr_.sg_list = NULL;
          // wr_.wr.rdma.remote_addr = addr;
          // wr_.wr.rdma.rkey = rkey;
          wr_.send_flags = IBV_SEND_SIGNALED;
          wr_.bind_mw.mw = mw_;
          wr_.bind_mw.rkey = newkey;
          wr_.bind_mw.bind_info.addr = addr;
          wr_.bind_mw.bind_info.length = size;
          wr_.bind_mw.bind_info.mr = global_mr_;
          wr_.bind_mw.bind_info.mw_access_flags = IBV_ACCESS_REMOTE_READ | 
                                    IBV_ACCESS_REMOTE_WRITE;
          printf("try to bind with rkey: %d, old_rkey is %d, old mw is %d\n", newkey, rkey, mw_->rkey);
          if (ibv_post_send(work_info->cm_id->qp, &wr_, &bad_wr_)) {
            perror("ibv_post_send mw_bind fail");
            resp_msg->status = RES_FAIL;
          } else {
              while (true) {
                ibv_wc wc;
                int rc = ibv_poll_cq(work_info->cq, 1, &wc);
                if (rc > 0) {
                  if (IBV_WC_SUCCESS == wc.status) {
                    // Break out as operation completed successfully
                    // printf("Break out as operation completed successfully\n");
                    resp_msg->status = RES_OK;
                    resp_msg->addr = addr;
                    resp_msg->rkey = wr_.bind_mw.rkey;
                    resp_msg->size = size;
                    printf("bind success! rkey = %d, mw.rkey = %d \n", wr_.bind_mw.rkey, mw_->rkey);
                    break;
                  } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
                    perror("cmd_send IBV_WC_WR_FLUSH_ERR");
                    resp_msg->status = RES_FAIL;
                    break;
                  } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
                    perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
                    resp_msg->status = RES_FAIL;
                    break;
                  } else {
                    perror("cmd_send ibv_poll_cq status error");
                    printf("%d\n", wc.status);
                    resp_msg->status = RES_FAIL;
                    break;
                  }
                } else if (0 == rc) {
                  continue;
                } else {
                  perror("ibv_poll_cq fail");
                  resp_msg->status = RES_FAIL;
                  break;
                }
              }
          }
        }
      } else {
        perror("recv wrong rkey");
        resp_msg->status = RES_FAIL;
      }
      // printf("fetch 2MB memory, addr: %ld, rkey: %d\n", resp_msg->addr,
      //        resp_msg->rkey);
      /* write response */
      remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
                   sizeof(CmdMsgRespBlock), request->resp_addr,
                   request->resp_rkey);

    }
    else if (request->type == RPC_FUSEE_SUBTABLE){
      uint64_t addr = rpc_fusee_->mm_alloc_subtable();
      uint32_t rkey = rpc_fusee_->get_rkey();
      FuseeSubtableResponse* resp_msg = (FuseeSubtableResponse*)cmd_resp;
      if(addr != 0){
        resp_msg->addr = addr;
        resp_msg->rkey = rkey;
        resp_msg->status = RES_OK;
      } else {
        resp_msg->status = RES_FAIL;
      }
      remote_write(work_info, (uint64_t)cmd_resp, resp_mr->lkey,
        sizeof(CmdMsgRespBlock), request->resp_addr,
        request->resp_rkey);
    }
    else if (request->type == MSG_UNREGISTER) {
      /* handle memory unregister requests */
      UnregisterRequest *unreg_req = (UnregisterRequest *)request;
      printf("receive a memory unregister message, addr: %ld\n",
             unreg_req->addr);
      // TODO: implemente memory unregister
    } else {
      printf("wrong request type\n");
    }
  }
}

}  // namespace kv
