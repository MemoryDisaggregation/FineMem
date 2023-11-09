#include "rdma_conn.h"
#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <cstdio>
#include <type_traits>
#include "free_block_manager.h"
#include "msg.h"

namespace mralloc {

int RDMAConnection::init_async() {
  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return -1;
  }

  if (rdma_create_id(m_cm_channel_, &m_cm_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return -1;
  }

  m_pd_ = ibv_alloc_pd(m_cm_id_->verbs);
  if (!m_pd_) {
    perror("ibv_alloc_pd fail");
    return -1;
  }

  struct ibv_comp_channel *comp_chan;
  comp_chan = ibv_create_comp_channel(m_cm_id_->verbs);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  m_cq_ = ibv_create_cq(m_cm_id_->verbs, 2, NULL, comp_chan, 0);
  if (!m_cq_) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(m_cq_, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 2;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 1;

  qp_attr.send_cq = m_cq_;
  qp_attr.recv_cq = m_cq_;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(m_cm_id_, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }
  return 0;
}

int RDMAConnection::connect_async(const std::string ip, const std::string port, uint8_t access_type) {

  struct addrinfo *res;
  if (getaddrinfo(ip.c_str(), port.c_str(), NULL, &res) < 0) {
    perror("getaddrinfo fail");
    return -1;
  }

  struct addrinfo *t = nullptr;
  for (t = res; t; t = t->ai_next) {
    if (!rdma_resolve_addr(m_cm_id_, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS)) {
      break;
    }
  }
  if (!t) {
    perror("getaddrdma_resolve_addrrinfo fail");
    return -1;
  }

  struct rdma_cm_event *event;
  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(m_cm_id_, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return 1;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    printf("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  uint8_t access_type_ = access_type;
  struct rdma_conn_param conn_param = {};
  conn_param.responder_resources = 1;
  conn_param.private_data = &access_type_;
  conn_param.private_data_len = sizeof(access_type_);
  conn_param.initiator_depth = 1;
  conn_param.retry_count = 7;
  if (rdma_connect(m_cm_id_, &conn_param)) {
    perror("rdma_connect fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    return -1;
  }

  struct PData server_pdata;
  memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

  rdma_ack_cm_event(event);

  m_server_cmd_msg_ = server_pdata.buf_addr;
  m_server_cmd_rkey_ = server_pdata.buf_rkey;
  if (access_type == CONN_FUSEE){
    m_fusee_rkey = server_pdata.buf_rkey;
  }
  assert(server_pdata.size == sizeof(CmdMsgBlock));

  // printf("private data, addr: %ld: rkey:%d, size: %d\n",
  // server_pdata.buf_addr,
  //        server_pdata.buf_rkey, server_pdata.size);

  m_cmd_msg_ = new CmdMsgBlock();
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  m_msg_mr_ = rdma_register_memory((void *)m_cmd_msg_, sizeof(CmdMsgBlock));
  if (!m_msg_mr_) {
    perror("ibv_reg_mr m_msg_mr_ fail");
    return -1;
  }

  m_cmd_resp_ = new CmdMsgRespBlock();
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_resp_mr_ =
      rdma_register_memory((void *)m_cmd_resp_, sizeof(CmdMsgRespBlock));
  if (!m_resp_mr_) {
    perror("ibv_reg_mr m_resp_mr_ fail");
    return -1;
  }

  m_reg_buf_ = new char[MAX_REMOTE_SIZE];
  m_reg_buf_mr_ = rdma_register_memory((void *)m_reg_buf_, MAX_REMOTE_SIZE);
  if (!m_reg_buf_mr_) {
    perror("ibv_reg_mr m_reg_buf_mr_ fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::init(const std::string ip, const std::string port, ibv_context* ctx, ibv_pd* pd, ibv_cq* cq, uint8_t access_type) {
  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return -1;
  }

  if (rdma_create_id(m_cm_channel_, &m_cm_id_, ctx, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return -1;
  }

  struct addrinfo *res;
  if (getaddrinfo(ip.c_str(), port.c_str(), NULL, &res) < 0) {
    perror("getaddrinfo fail");
    return -1;
  }

  struct addrinfo *t = nullptr;
  for (t = res; t; t = t->ai_next) {
    if (!rdma_resolve_addr(m_cm_id_, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS)) {
      break;
    }
  }
  if (!t) {
    perror("getaddrdma_resolve_addrrinfo fail");
    return -1;
  }

  struct rdma_cm_event *event;
  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(m_cm_id_, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return 1;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    printf("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);
  if(pd) {
    m_pd_ = pd;
  } else {
    m_pd_ = ibv_alloc_pd(m_cm_id_->verbs);
    if (!m_pd_) {
      perror("ibv_alloc_pd fail");
      return -1;
    }
  }

  struct ibv_comp_channel *comp_chan;
  comp_chan = ibv_create_comp_channel(m_cm_id_->verbs);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  if (cq) {
    m_cq_ = cq;
  } else {
    m_cq_ = ibv_create_cq(m_cm_id_->verbs, 2, NULL, comp_chan, 0);
    if (!m_cq_) {
      perror("ibv_create_cq fail");
      return -1;
    }
    

    if (ibv_req_notify_cq(m_cq_, 0)) {
      perror("ibv_req_notify_cq fail");
      return -1;
    }
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 2;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 1;

  qp_attr.send_cq = m_cq_;
  qp_attr.recv_cq = m_cq_;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(m_cm_id_, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  uint8_t access_type_ = access_type;
  struct rdma_conn_param conn_param = {};
  conn_param.responder_resources = 1;
  conn_param.private_data = &access_type_;
  conn_param.private_data_len = sizeof(access_type_);
  conn_param.initiator_depth = 1;
  conn_param.retry_count = 7;
  if (rdma_connect(m_cm_id_, &conn_param)) {
    perror("rdma_connect fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    return -1;
  }

  struct PData server_pdata;
  memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

  rdma_ack_cm_event(event);

  m_server_cmd_msg_ = server_pdata.buf_addr;
  m_server_cmd_rkey_ = server_pdata.buf_rkey;
  if (access_type == CONN_FUSEE){
    m_fusee_rkey = server_pdata.buf_rkey;
  }
  assert(server_pdata.size == sizeof(CmdMsgBlock));

  // printf("private data, addr: %ld: rkey:%d, size: %d\n",
  // server_pdata.buf_addr,
  //        server_pdata.buf_rkey, server_pdata.size);

  m_cmd_msg_ = new CmdMsgBlock();
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  m_msg_mr_ = rdma_register_memory((void *)m_cmd_msg_, sizeof(CmdMsgBlock));
  if (!m_msg_mr_) {
    perror("ibv_reg_mr m_msg_mr_ fail");
    return -1;
  }

  m_cmd_resp_ = new CmdMsgRespBlock();
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_resp_mr_ =
      rdma_register_memory((void *)m_cmd_resp_, sizeof(CmdMsgRespBlock));
  if (!m_resp_mr_) {
    perror("ibv_reg_mr m_resp_mr_ fail");
    return -1;
  }

  m_reg_buf_ = new char[MAX_REMOTE_SIZE];
  m_reg_buf_mr_ = rdma_register_memory((void *)m_reg_buf_, MAX_REMOTE_SIZE);
  if (!m_reg_buf_mr_) {
    perror("ibv_reg_mr m_reg_buf_mr_ fail");
    return -1;
  }

  return 0;
}

int RDMAConnection::init(const std::string ip, const std::string port, uint8_t access_type) {
  m_cm_channel_ = rdma_create_event_channel();
  if (!m_cm_channel_) {
    perror("rdma_create_event_channel fail");
    return -1;
  }

  if (rdma_create_id(m_cm_channel_, &m_cm_id_, NULL, RDMA_PS_TCP)) {
    perror("rdma_create_id fail");
    return -1;
  }

  struct addrinfo *res;
  if (getaddrinfo(ip.c_str(), port.c_str(), NULL, &res) < 0) {
    perror("getaddrinfo fail");
    return -1;
  }

  struct addrinfo *t = nullptr;
  for (t = res; t; t = t->ai_next) {
    if (!rdma_resolve_addr(m_cm_id_, NULL, t->ai_addr, RESOLVE_TIMEOUT_MS)) {
      break;
    }
  }
  if (!t) {
    perror("getaddrdma_resolve_addrrinfo fail");
    return -1;
  }

  struct rdma_cm_event *event;
  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    perror("RDMA_CM_EVENT_ADDR_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  if (rdma_resolve_route(m_cm_id_, RESOLVE_TIMEOUT_MS)) {
    perror("rdma_resolve_route fail");
    return -1;
  }

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return 1;
  }

  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    printf("aaa: %d\n", event->event);
    perror("RDMA_CM_EVENT_ROUTE_RESOLVED fail");
    return -1;
  }

  rdma_ack_cm_event(event);

  m_pd_ = ibv_alloc_pd(m_cm_id_->verbs);
  if (!m_pd_) {
    perror("ibv_alloc_pd fail");
    return -1;
  }

  struct ibv_comp_channel *comp_chan;
  comp_chan = ibv_create_comp_channel(m_cm_id_->verbs);
  if (!comp_chan) {
    perror("ibv_create_comp_channel fail");
    return -1;
  }

  m_cq_ = ibv_create_cq(m_cm_id_->verbs, 1024, NULL, comp_chan, 0);
  if (!m_cq_) {
    perror("ibv_create_cq fail");
    return -1;
  }

  if (ibv_req_notify_cq(m_cq_, 0)) {
    perror("ibv_req_notify_cq fail");
    return -1;
  }

  struct ibv_qp_init_attr qp_attr = {};
  qp_attr.cap.max_send_wr = 512;
  qp_attr.cap.max_send_sge = 16;
  qp_attr.cap.max_recv_wr = 1;
  qp_attr.cap.max_recv_sge = 16;
  qp_attr.sq_sig_all = 0;


  qp_attr.send_cq = m_cq_;
  qp_attr.recv_cq = m_cq_;
  qp_attr.qp_type = IBV_QPT_RC;
  if (rdma_create_qp(m_cm_id_, m_pd_, &qp_attr)) {
    perror("rdma_create_qp fail");
    return -1;
  }

  uint8_t access_type_ = access_type;
  struct rdma_conn_param conn_param = {};
  conn_param.responder_resources = 16;
  conn_param.private_data = &access_type_;
  conn_param.private_data_len = sizeof(access_type_);
  conn_param.initiator_depth = 16;
  conn_param.retry_count = 7;
  if (rdma_connect(m_cm_id_, &conn_param)) {
    perror("rdma_connect fail");
    return -1;
  }

  // printf("start\n");

  if (rdma_get_cm_event(m_cm_channel_, &event)) {
    perror("rdma_get_cm_event fail");
    return -1;
  }
  // printf("end\n");

  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    perror("RDMA_CM_EVENT_ESTABLISHED fail");
    return -1;
  }

  struct PData server_pdata;
  memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

  rdma_ack_cm_event(event);

  m_server_cmd_msg_ = server_pdata.buf_addr;
  m_server_cmd_rkey_ = server_pdata.buf_rkey;
  // if (access_type == CONN_FUSEE){
  //   m_fusee_rkey =  server_pdata.buf_rkey;
  // }
  m_one_side_info_ = {server_pdata.header_addr, 
                      server_pdata.rkey_addr,
                      server_pdata.block_addr, 
                      server_pdata.block_num, 
                      server_pdata.base_size, 
                      server_pdata.fast_size};
  global_rkey_ = server_pdata.global_rkey;
  conn_id_ = server_pdata.id;
  assert(server_pdata.size == sizeof(CmdMsgBlock));

  // printf("private data, addr: %ld: rkey:%d, size: %d\n",
  // server_pdata.buf_addr,
  //        server_pdata.buf_rkey, server_pdata.size);

  m_cmd_msg_ = new CmdMsgBlock();
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  m_msg_mr_ = rdma_register_memory((void *)m_cmd_msg_, sizeof(CmdMsgBlock));
  if (!m_msg_mr_) {
    perror("ibv_reg_mr m_msg_mr_ fail");
    return -1;
  }

  m_cmd_resp_ = new CmdMsgRespBlock();
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_resp_mr_ =
      rdma_register_memory((void *)m_cmd_resp_, sizeof(CmdMsgRespBlock));
  if (!m_resp_mr_) {
    perror("ibv_reg_mr m_resp_mr_ fail");
    return -1;
  }

  m_reg_buf_ = new char[MAX_REMOTE_SIZE];
  m_reg_buf_mr_ = rdma_register_memory((void *)m_reg_buf_, MAX_REMOTE_SIZE);
  if (!m_reg_buf_mr_) {
    perror("ibv_reg_mr m_reg_buf_mr_ fail");
    return -1;
  }

  // TODO: add support for one_sided
  // header_list = (block_header_e*)malloc(m_one_side_info_.m_block_num*sizeof(block_header));
  // rkey_list = (uint32_t*)malloc(m_one_side_info_.m_block_num*sizeof(uint32_t));
  // update_mem_metadata();
  // update_rkey_metadata();
  last_alloc_ = 0;
  full_bitmap = (bool*)malloc(m_one_side_info_.m_block_num*sizeof(bool));

  return 0;
}

struct ibv_mr *RDMAConnection::rdma_register_memory(void *ptr, uint64_t size) {
  struct ibv_mr *mr =
      ibv_reg_mr(m_pd_, ptr, size,
                 IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE);
  if (!mr) {
    perror("ibv_reg_mr fail");
    return nullptr;
  }
  return mr;
}

int RDMAConnection::rdma_remote_read(uint64_t local_addr, uint32_t lkey,
                                     uint64_t length, uint64_t remote_addr,
                                     uint32_t rkey) {
  struct ibv_sge sge;
  sge.addr = (uintptr_t)local_addr;
  sge.length = length;
  sge.lkey = lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = IBV_WR_RDMA_READ;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = remote_addr;
  send_wr.wr.rdma.rkey = rkey;
  if (ibv_post_send(m_cm_id_->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // printf("remote read %ld %d\n", remote_addr, rkey);
  auto start = TIME_NOW;
  int ret = -1;
  struct ibv_wc wc;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("rdma_remote_read timeout\n");
      return -1;
    }
    int rc = ibv_poll_cq(m_cq_, 1, &wc);
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
        printf("%d\n", wc.status);
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

int RDMAConnection::rdma_remote_write(uint64_t local_addr, uint32_t lkey,
                                      uint64_t length, uint64_t remote_addr,
                                      uint32_t rkey) {
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
  if (ibv_post_send(m_cm_id_->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // printf("remote write %ld %d\n", remote_addr, rkey);

  auto start = TIME_NOW;
  int ret = -1;
  struct ibv_wc wc;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("rdma_remote_write timeout\n");
      return -1;
    }
    int rc = ibv_poll_cq(m_cq_, 1, &wc);
    if (rc > 0) {
      if (IBV_WC_SUCCESS == wc.status) {
        // Break out as operation completed successfully
        // printf("Break out as operation completed successfully\n");
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
        printf("%d\n", wc.status);
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

int RDMAConnection::remote_read(void *ptr, uint64_t size, uint64_t remote_addr,
                                uint32_t rkey) {
  int ret = rdma_remote_read((uint64_t)m_reg_buf_, m_reg_buf_mr_->lkey, size,
                             remote_addr, rkey);
  if (ret) {
    return -1;
  }
  memcpy(ptr, m_reg_buf_, size);
  return ret;
}

int RDMAConnection::remote_write(void *ptr, uint64_t size, uint64_t remote_addr,
                                 uint32_t rkey) {
  memcpy(m_reg_buf_, ptr, size);
  return rdma_remote_write((uint64_t)m_reg_buf_, m_reg_buf_mr_->lkey, size,
                           remote_addr, rkey);
}

uint64_t RDMAConnection::remote_CAS(uint64_t swap, uint64_t compare, uint64_t remote_addr, uint32_t rkey) {

  struct ibv_sge sge;
  sge.addr = (uintptr_t)m_reg_buf_;
  sge.length = sizeof(uint64_t);
  sge.lkey = m_reg_buf_mr_->lkey;

  struct ibv_send_wr send_wr = {};
  struct ibv_send_wr *bad_send_wr;
  send_wr.wr_id = 0;
  send_wr.num_sge = 1;
  send_wr.next = NULL;
  send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
  send_wr.sg_list = &sge;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.atomic.remote_addr = remote_addr;
  send_wr.wr.atomic.rkey = rkey;
  send_wr.wr.atomic.compare_add = compare;
  send_wr.wr.atomic.swap = swap;
  if (ibv_post_send(m_cm_id_->qp, &send_wr, &bad_send_wr)) {
    perror("ibv_post_send fail");
    return -1;
  }

  // printf("remote write %ld %d\n", remote_addr, rkey);

  auto start = TIME_NOW;
  int ret = -1;
  struct ibv_wc wc;
  while (true) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("rdma_remote_write timeout\n");
      return -1;
    }
    int rc = ibv_poll_cq(m_cq_, 1, &wc);
    if (rc > 0) {
      if (IBV_WC_SUCCESS == wc.status) {
        // Break out as operation completed successfully
        // printf("Break out as operation completed successfully\n");
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
        printf("%d\n", wc.status);
        break;
      }
    } else if (0 == rc) {
      continue;
    } else {
      perror("ibv_poll_cq fail");
      break;
    }
  }
  return *((uint64_t*)m_reg_buf_);
}

int RDMAConnection::register_remote_memory(uint64_t &addr, uint32_t &rkey,
                                           uint64_t size) {
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_cmd_resp_->notify = NOTIFY_IDLE;
  RegisterRequest *request = (RegisterRequest *)m_cmd_msg_;
  request->resp_addr = (uint64_t)m_cmd_resp_;
  request->resp_rkey = m_resp_mr_->rkey;
  request->id = conn_id_;
  request->type = MSG_REGISTER;
  request->size = size;
  m_cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = rdma_remote_write((uint64_t)m_cmd_msg_, m_msg_mr_->lkey,
                              sizeof(CmdMsgBlock), m_server_cmd_msg_,
                              m_server_cmd_rkey_);
  if (ret) {
    printf("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (m_cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("wait for request completion timeout\n");
      return -1;
    }
  }
  RegisterResponse *resp_msg = (RegisterResponse *)m_cmd_resp_;
  if (resp_msg->status != RES_OK) {
    printf("register remote memory fail\n");
    return -1;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

int RDMAConnection::remote_fetch_fast_block(uint64_t &addr, uint32_t &rkey){
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_cmd_resp_->notify = NOTIFY_IDLE;
  RequestsMsg *request = (RequestsMsg *)m_cmd_msg_;
  request->resp_addr = (uint64_t)m_cmd_resp_;
  request->resp_rkey = m_resp_mr_->rkey;
  request->id = conn_id_;
  request->type = MSG_FETCH_FAST;
  m_cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = rdma_remote_write((uint64_t)m_cmd_msg_, m_msg_mr_->lkey,
                              sizeof(CmdMsgBlock), m_server_cmd_msg_,
                              m_server_cmd_rkey_);
  if (ret) {
    printf("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (m_cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("wait for request completion timeout\n");
      return -1;
    }
  }
  FetchFastResponse *resp_msg = (FetchFastResponse *)m_cmd_resp_;
  if (resp_msg->status != RES_OK) {
    printf("fetch fast block fail\n");
    return -1;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

int RDMAConnection::remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey){
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_cmd_resp_->notify = NOTIFY_IDLE;
  MWbindRequest *request = (MWbindRequest *)m_cmd_msg_;
  request->resp_addr = (uint64_t)m_cmd_resp_;
  request->resp_rkey = m_resp_mr_->rkey;
  request->id = conn_id_;
  request->type = MSG_MW_BIND;
  request->size = size;
  request->rkey = rkey;
  request->addr = addr;
  request->newkey = newkey;
  m_cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = rdma_remote_write((uint64_t)m_cmd_msg_, m_msg_mr_->lkey,
                              sizeof(CmdMsgBlock), m_server_cmd_msg_,
                              m_server_cmd_rkey_);
  if (ret) {
    printf("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (m_cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("wait for request completion timeout\n");
      return -1;
    }
  }
  MWbindResponse *resp_msg = (MWbindResponse *)m_cmd_resp_;
  if (resp_msg->status != RES_OK) {
    printf("mem window bind fail\n");
    return -1;
  }
  newkey = resp_msg->rkey;

  return 0;
}

int RDMAConnection::remote_fetch_block(uint64_t &addr, uint32_t &rkey, uint64_t size) {
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_cmd_resp_->notify = NOTIFY_IDLE;
  FetchRequest *request = (FetchRequest *)m_cmd_msg_;
  request->resp_addr = (uint64_t)m_cmd_resp_;
  request->resp_rkey = m_resp_mr_->rkey;
  request->id = conn_id_;
  request->type = MSG_FETCH;
  request->size = size;
  m_cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = rdma_remote_write((uint64_t)m_cmd_msg_, m_msg_mr_->lkey,
                              sizeof(CmdMsgBlock), m_server_cmd_msg_,
                              m_server_cmd_rkey_);
  if (ret) {
    printf("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (m_cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("wait for request completion timeout\n");
      return -1;
    }
  }
  FetchResponse *resp_msg = (FetchResponse *)m_cmd_resp_;
  if (resp_msg->status != RES_OK || size != resp_msg->size) {
    printf("fetch fast block fail\n");
    return -1;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

int RDMAConnection::remote_fusee_alloc(uint64_t &addr, uint32_t &rkey){
  memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
  memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
  m_cmd_resp_->notify = NOTIFY_IDLE;
  RequestsMsg *request = (RequestsMsg *)m_cmd_msg_;
  request->resp_addr = (uint64_t)m_cmd_resp_;
  request->resp_rkey = m_resp_mr_->rkey;
  request->id = conn_id_;
  request->type = RPC_FUSEE_SUBTABLE;
  m_cmd_msg_->notify = NOTIFY_WORK;

  /* send a request to sever */
  int ret = rdma_remote_write((uint64_t)m_cmd_msg_, m_msg_mr_->lkey,
                              sizeof(CmdMsgBlock), m_server_cmd_msg_,
                              m_server_cmd_rkey_);
  if (ret) {
    printf("fail to send requests\n");
    return ret;
  }

  /* wait for response */
  auto start = TIME_NOW;
  while (m_cmd_resp_->notify == NOTIFY_IDLE) {
    if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US) {
      printf("wait for request completion timeout\n");
      return -1;
    }
  }
  FuseeSubtableResponse *resp_msg = (FuseeSubtableResponse *)m_cmd_resp_;
  if (resp_msg->status != RES_OK) {
    printf("fetch fast block fail\n");
    return -1;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

bool RDMAConnection::update_rkey_metadata() {
  if(rkey_list != 0){
    uint64_t rkey_size = m_one_side_info_.m_block_num * sizeof(uint32_t);
    remote_read(rkey_list, rkey_size, m_one_side_info_.m_rkey_addr_, get_global_rkey());
    return true;
  }
  return false;
}

bool RDMAConnection::update_mem_metadata(uint64_t index) {
  uint64_t metadata_size = sizeof(large_block);
  assert(sizeof(large_block) == sizeof(large_block_lockless));
  remote_read(&block_, metadata_size, m_one_side_info_.m_header_addr_ + index*sizeof(large_block), get_global_rkey());
  return true;
}

bool RDMAConnection::update_mem_bitmap(uint64_t index) {
  assert(sizeof(large_block) == sizeof(large_block_lockless));
  remote_read(&block_.bitmap, sizeof(uint64_t), m_one_side_info_.m_header_addr_ + index*sizeof(large_block), get_global_rkey());
  return true;
}


bool RDMAConnection::malloc_hint(uint64_t start, uint64_t idx) {
  user_start_ = (start - m_one_side_info_.m_block_addr_)/m_one_side_info_.m_fast_size/large_block_items;
  last_alloc_ = idx%40 * (m_one_side_info_.m_block_num - user_start_ - 1)/40;
  if(user_start_ > m_one_side_info_.m_block_num || last_alloc_ > m_one_side_info_.m_block_num)
    return false;
  printf("usert_start_=%lu, last_alloc_=%lu\n", user_start_, last_alloc_);
  update_mem_metadata((last_alloc_)%(m_one_side_info_.m_block_num-user_start_) + user_start_);
  return true;
}

bool RDMAConnection::fetch_mem_one_sided(uint64_t &addr, uint32_t &rkey) {
  uint64_t block_num = m_one_side_info_.m_block_num;
  uint64_t fast_size = m_one_side_info_.m_fast_size;
  uint64_t base_size = m_one_side_info_.m_base_size;
  int old_item = 0;
  uint64_t free_index;
  for(int i = 0; i < block_num-user_start_; i++) {
    while(block_.bitmap != ~(uint64_t)0) {
        // find valid bit, try to allocate 
        uint64_t result;
        do{
            free_index = find_free_index_from_bitmap(block_.bitmap);
            uint64_t bitmap_ = block_.bitmap;
            bitmap_ |= (uint64_t)1<<free_index;
            result = remote_CAS(bitmap_, block_.bitmap, 
              m_one_side_info_.m_header_addr_ + block_.offset*sizeof(large_block),
              get_global_rkey());
            // printf("bitmap result = %lu\n", result);
        } while (result != block_.bitmap && (block_.bitmap = result) != ~(uint64_t)0);
        if(block_.bitmap == ~(uint64_t)0) {
          // printf("block full\n");
          break;
        }
        // if(block_.header[free_index].flag % 2 == 1){
        //   printf("old value!\n");
        //   update_mem_metadata(block_.offset);
        //   continue;
        // }
        // block_header_e header_new = block_.header[free_index];
        // header_new.flag |= 1;
        // // printf("old value:%lu\n", *(uint64_t*)&block_.header[free_index]);
        // result = remote_CAS(*(uint64_t*)&header_new, *(uint64_t*)&block_.header[free_index], 
        //     m_one_side_info_.m_header_addr_ + block_.offset*sizeof(large_block) + sizeof(uint64_t) + sizeof(block_header_e)*free_index,
        //     get_global_rkey());
        // if (result != *(uint64_t*)&block_.header[free_index]) {
        //   printf("block header %lu result = %lu\n", free_index, result);
        //   old_item++;
        //   update_mem_metadata(block_.offset);
        // } else {
          addr = m_one_side_info_.m_block_addr_ + (block_.offset * large_block_items + free_index) * m_one_side_info_.m_fast_size;
          // rkey = rkey_list[index];
          rkey = get_global_rkey();
          // printf("addr:%lx, rkey:%u\n", addr, rkey);
          return true;
        // }
    }
    full_bitmap[block_.offset] = true;
    int offset = (block_.offset - user_start_ + 1)%(block_num-user_start_) + user_start_;
    while(full_bitmap[offset]){
      offset = (offset - user_start_ + 1)%(block_num-user_start_) + user_start_;
    }
    block_.offset = offset;
    update_mem_bitmap(offset);
    // printf("costly operations\n");
    // update_mem_metadata(offset);
  }
  // for(int i = 0; i< block_num-user_start_; i++){
  //   uint64_t index = (i+last_alloc_)%(block_num-user_start_) + user_start_;
  //   if(header_list[index].max_length == fast_size/base_size && (header_list[index].flag & (uint64_t)1) == 1){
  //     block_header_e update_header = header_list[index];
  //     update_header.flag &= ~((uint64_t)1);
  //     uint64_t swap_value = *(uint64_t*)(&update_header); 
  //     uint64_t cmp_value = *(uint64_t*)(&header_list[index]);
  //     uint64_t result = remote_CAS(swap_value, cmp_value, 
  //                                                 m_one_side_info_.m_header_addr_ + index * sizeof(block_header), 
  //                                                 get_global_rkey());
  //     if (result != cmp_value) {
  //       old_item++;
  //       // total_old_++;
  //       if(old_item % 100 == 0){
  //         // printf("total out of time:%d, update metadata\n", old_item);
  //         update_mem_metadata();
  //       }
  //     } else {
  //       last_alloc_ = index + 1;
  //       addr = m_one_side_info_.m_block_addr_ + index * m_one_side_info_.m_fast_size;
  //       // rkey = rkey_list[index];
  //       rkey = get_global_rkey();
  //       // printf("addr:%lx, rkey:%u\n", addr, rkey);
  //       return true;
  //     }
  //   }
  // }
  return false;
}

}  // namespace kv
