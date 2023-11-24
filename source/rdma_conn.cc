#include "rdma_conn.h"
#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <cstdio>
#include <type_traits>
#include "free_block_manager.h"
#include "msg.h"

namespace mralloc {

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
    m_one_side_info_ = {server_pdata.block_size_,
                        server_pdata.block_num_,
                        server_pdata.global_rkey_, 
                        server_pdata.section_header_, 
                        server_pdata.heap_start_};
    global_rkey_ = server_pdata.global_rkey_;
    conn_id_ = server_pdata.id;
    
    block_size_ = server_pdata.block_size_;
    block_num_ = server_pdata.block_num_;
    region_size_ = block_size_ * block_per_region;
    region_num_ = block_num_ / block_per_region;
    section_size_ = region_size_ * region_per_section;
    section_num_ = region_num_ / region_per_section;

    section_header_ = server_pdata.section_header_;
    fast_region_ = (uint64_t)((section_e*)section_header_ + section_num_);
    region_header_ = (uint64_t)((fast_class*)fast_region_ + block_class_num*section_num_);
    block_rkey_ = (uint64_t)((region_e*)region_header_ + region_num_);
    class_block_rkey_ = (uint64_t)((uint32_t*)block_rkey_ + block_num_);
    heap_start_ = server_pdata.heap_start_;

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
    //   last_alloc_ = 0;
    //   full_bitmap = (bool*)malloc(m_one_side_info_.m_block_num*sizeof(bool));

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

bool RDMAConnection::remote_CAS(uint64_t swap, uint64_t *compare, uint64_t remote_addr, uint32_t rkey) {

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
    send_wr.wr.atomic.compare_add = *compare;
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
        // printf("busy waiting\n");
        
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
    if(*compare != *((uint64_t*)m_reg_buf_)){
        *compare = *((uint64_t*)m_reg_buf_);
        return false;
    }
    return true;
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

int RDMAConnection::remote_fetch_block(uint64_t &addr, uint32_t &rkey){
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
  FetchBlockResponse *resp_msg = (FetchBlockResponse *)m_cmd_resp_;
  if (resp_msg->status != RES_OK) {
    printf("fetch block block fail\n");
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
    printf("fetch block block fail\n");
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
    printf("fetch block block fail\n");
    return -1;
  }
  addr = resp_msg->addr;
  rkey = resp_msg->rkey;
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
  return 0;
}

bool RDMAConnection::update_section(region_e region, alloc_advise advise) {
    uint64_t section_offset = region.offset_/region_per_section;
    uint64_t region_offset = region.offset_%region_per_section;
    section_e section_old;
    remote_read(&section_old, sizeof(section_old), section_metadata_addr(section_offset), global_rkey_);
    section_e section_new = section_old;
    if(advise == alloc_exclusive) {
        do{
            if(((section_old.alloc_map_ & section_old.class_map_) & 1<<region_offset) != 0){
                return false;
            }
            section_new.alloc_map_ |= 1 << region_offset;
            section_new.class_map_ |= 1 << region_offset;
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_empty) {
        do{
            if((~(section_old.alloc_map_ | section_old.class_map_) & 1<<region_offset) != 0){
                return false;
            }
            section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
            section_new.class_map_ &= ~((bitmap32)1 << region_offset);
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_no_class) {
        do{
            if(((section_old.alloc_map_ & ~section_old.class_map_) & 1<<region_offset) != 0){
                return false;
            }
            section_new.class_map_ &= ~((bitmap32)1 << region_offset);
            section_new.alloc_map_ |= 1 << region_offset;
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_class) {
        do{
            if(((~section_old.alloc_map_ & section_old.class_map_) & 1<<region_offset) != 0){
                return false;
            }
            section_new.class_map_ |= 1 << region_offset;
            section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    }
    return false;
}

bool RDMAConnection::find_section(section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) {
    section_e section;
    if(advise == alloc_class) {
        for(int i = section_num_ - 1; i >= 0; i--) {
            remote_read(&section, sizeof(section_e), section_metadata_addr(i), global_rkey_);
            if((section.class_map_ | section.alloc_map_) != ~(uint32_t)0){
                alloc_section = section;
                section_offset = i;
                return true;
            }
        }
    } else if(advise == alloc_no_class) {
        for(int i = section_num_ - 1; i >= 0; i--) {
            remote_read(&section, sizeof(section_e), section_metadata_addr(i), global_rkey_);
            if((section.class_map_ | section.alloc_map_)  != ~(uint32_t)0){
                alloc_section = section;
                section_offset = i;
                return true;
            }
        }
    } else if (advise == alloc_empty) {
        for(int i = 0; i < section_num_; i++) {
            remote_read(&section, sizeof(section_e), section_metadata_addr(i), global_rkey_);
            if((section.class_map_ | section.alloc_map_ ) != ~(uint32_t)0){
                alloc_section = section;
                section_offset = i;
                return true;
            }
        }
    } else { return false; }
    return false;
}

bool RDMAConnection::fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region) {
    if(block_class == 0 && shared == true) {
        // force use unclassed one to alloc single block
        section_e new_section;
        uint32_t free_map;
        int index;
        do {
            free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                free_map = alloc_section.class_map_;
                if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
            }
            // if first alloc, do state update
            if(((alloc_section.alloc_map_>>index) & 1) != 0) {
                break;
            }
            new_section = alloc_section;
            new_section.alloc_map_ |= (1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        remote_read(&alloc_region, sizeof(region_e), region_metadata_addr(section_offset*region_per_section+index), global_rkey_);
        return true;
    }
    else if(block_class == 0 && shared == false) {
        section_e new_section;
        uint32_t free_map;
        int index;
        do {
            free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                free_map = alloc_section.class_map_;
                if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
            }
            new_section = alloc_section;
            new_section.alloc_map_ |= (1<<index);
            new_section.class_map_ |= (1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        region_e region_new, region_old;
        remote_read(&region_old, sizeof(region_e), region_metadata_addr(section_offset*region_per_section+index), global_rkey_);
        do {
            region_new = region_old;
            if(region_new.exclusive_ == 1) {
                printf("impossible problem: exclusive is already set\n");
                return false;
            }
            region_new.exclusive_ = 1;
        }while(!remote_CAS(*(uint64_t*)&region_new, (uint64_t*)&region_old, region_metadata_addr(region_new.offset_), global_rkey_));
        region_old = region_new;
        alloc_region = region_old;
        return true;
    }
    // class alloc, shared
    else if (shared == true) {
        int index;
        uint32_t fast_index = get_fast_region_index(section_offset, block_class);
        region_e region_new, region_old;
        fast_class_e class_region;
        remote_read(&class_region, sizeof(fast_class_e), fast_region_metadata_addr(fast_index), global_rkey_);
        for(int i = 0; i < 4;i++){
            if((index = class_region.offset[i]) != 0){
                remote_read(&region_old, sizeof(region_e), region_metadata_addr(index), global_rkey_);
                if(region_old.exclusive_ == 0 && region_old.block_class_ == block_class && region_old.class_map_ != bitmap16_filled) {
                    alloc_region = region_old;
                    return true;
                } else {
                    fast_class_e new_class_region;
                    new_class_region = class_region;
                    new_class_region.offset[i] = 0;
                    if(remote_CAS(*(uint64_t*)&new_class_region, (uint64_t*)&class_region, fast_region_metadata_addr(fast_index), global_rkey_)){
                        class_region = new_class_region;
                    }
                }
            }
        }
        uint32_t free_map;
        section_e new_section;
        do {
            free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                free_map = alloc_section.class_map_;
                if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
            }
            new_section = alloc_section;
            new_section.class_map_ |= (1<<index);
            new_section.alloc_map_ |= (0<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        remote_read(&region_old, sizeof(region_e), region_metadata_addr(section_offset*region_per_section+index), global_rkey_);
        // do {
        //     region_new = region_old;
        //     if(region_new.exclusive_ == 1) {
        //         printf("impossible problem: exclusive is already set\n");
        //         return false;
        //     }
        //     if(region_new.block_class_ != 0) {
        //         printf("impossible problem: class is already set\n");
        //         return false;
        //     }
        //     region_new.block_class_ = block_class;
        // } while (!region_header_[region_new.offset_].compare_exchange_strong(region_old, region_new));
        init_region_class(region_old, block_class, 0);
        alloc_region = region_old;
        return true;
    }
    // class alloc, and exclusive
    else {
        int index;
        region_e region_old, region_new;
        uint32_t free_map;
        section_e new_section;
        do {
            free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                free_map = alloc_section.class_map_;
                if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                    printf("section has no free space!\n");
                    return false;
                }
            }
            new_section = alloc_section;
            new_section.class_map_ |= (1<<index);
            new_section.alloc_map_ |= (1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        remote_read(&region_old, sizeof(region_e), region_metadata_addr(section_offset*region_per_section+index), global_rkey_);
        // do {
        //     region_new = region_old;
        //     if(region_new.exclusive_ == 1) {
        //         printf("impossible problem: exclusive is already set\n");
        //         return false;
        //     }
        //     if(region_new.block_class_ != 0) {
        //         printf("impossible problem: class is already set\n");
        //         return false;
        //     }
        //     region_new.block_class_ = block_class;
        //     region_new.exclusive_ = 1;
        // } while (!region_header_[region_new.offset_].compare_exchange_strong(region_old, region_new));
        init_region_class(region_old, block_class, 1);
        alloc_region = region_old;
        return true;
    }
    return false;
}

bool RDMAConnection::try_add_fast_region(uint32_t section_offset, uint32_t block_class, region_e &alloc_region){
    bool replace = true; uint32_t fast_index = get_fast_region_index(section_offset, block_class);
    fast_class_e class_region;
    remote_read(&class_region, sizeof(fast_class_e), fast_region_metadata_addr(fast_index), global_rkey_);
    fast_class_e new_class_region = class_region;
    for(int i = 0; i < 4;i++){
        if(class_region.offset[i] == 0) {
            replace = true;
            do {
                if(class_region.offset[i] != 0){
                    replace = false;
                    break;
                }
                new_class_region = class_region;
                new_class_region.offset[i] = alloc_region.offset_;
            }while(!remote_CAS(*(uint64_t*)&new_class_region, (uint64_t*)&class_region, fast_region_metadata_addr(fast_index), global_rkey_));
            if(replace) return true;
        }
    }
    return false;
}

bool RDMAConnection::fetch_large_region(section_e &alloc_section, uint32_t section_offset, uint64_t region_num, uint64_t &addr) {
    bitmap32 free_map = alloc_section.alloc_map_ | alloc_section.class_map_;
    int free_length = 0;
    // each section has 32 items
    for(int i = 0; i < 32; i++) {
        // a free space
        if(free_map%2 == 0) {
            free_length += 1;
            // length enough
            if(free_length == region_num) {
                section_e section_new = alloc_section;
                bitmap32 mask = 0;
                for(int j = i-free_length+1; j <= i; j++) {
                    mask |= 1 << j;
                }
                section_new.alloc_map_ |= mask;
                section_new.class_map_ |= mask;
                // find the section header changed
                if(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_)){
                    i = 0; free_length = 0; free_map = alloc_section.alloc_map_ | alloc_section.class_map_;
                    continue;
                }
                alloc_section = section_new;
                addr = get_section_region_addr(section_offset, i-free_length+1);
                return true;
            }
        } else {
            free_length = 0;
        }
        free_map >>= 1;
    } 
    return false;
}

bool RDMAConnection::set_region_exclusive(region_e &alloc_region) {
    if(!update_section(alloc_region, alloc_exclusive)){
        // already be set
        return false;
    }
    region_e new_region;
    do {
        new_region = alloc_region;
        if(new_region.exclusive_ == 1) {
            printf("impossible situation: exclusive has already been set\n");
            return false;
        }
        new_region.exclusive_ = 1;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(new_region.offset_), global_rkey_));
    alloc_region = new_region;
    return true;
}

bool RDMAConnection::set_region_empty(region_e &alloc_region) {
    if(alloc_region.exclusive_ != 1) {
        if(!set_region_exclusive(alloc_region))
            return false;
    }
    region_e new_region;
    do {
        new_region = alloc_region;
        if(new_region.base_map_ != 0) {
            printf("wait for free\n");
            return false;
        }
        new_region.exclusive_ = 0;
        new_region.block_class_ = 0;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(new_region.offset_), global_rkey_));
    alloc_region = new_region;
    if(!update_section(alloc_region, alloc_empty)){
        return false;
    }
    return true;
}

bool RDMAConnection::init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive) {
    // suppose the section has already set!
    region_e new_region;
    do {
        new_region = alloc_region;
        if((alloc_region.block_class_ != 0 && alloc_region.block_class_ != block_class) || alloc_region.exclusive_ != is_exclusive)  {
            return false;
        } 
        uint16_t mask = 0;
        uint32_t reader = new_region.base_map_;
        uint32_t tail = 1<<(block_class+1);
        for(int i = 0; i < block_per_region/(block_class+1); i++ ) {
            if(reader%tail == 0)
                mask |= 1<<i;
            reader >>= block_class+1;
        }
        new_region.class_map_ = ~mask;
        new_region.block_class_ = block_class;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(new_region.offset_), global_rkey_));
    alloc_region = new_region;
    return true;
}

bool RDMAConnection::fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive) {
    int index; region_e new_region;
    do{
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ != 0) {
            printf("state wrong, addr = %lx, exclusive = %d, class = %u\n", get_region_addr(alloc_region), alloc_region.exclusive_, alloc_region.block_class_);
            return false;
        } 
        new_region = alloc_region;
        if((index = find_free_index_from_bitmap32_lead(alloc_region.base_map_)) == -1) {
            return false;
        }
        new_region.base_map_ |= 1<<index;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(new_region.offset_), global_rkey_));
    alloc_region = new_region;
    addr = get_region_block_addr(alloc_region, index);
    rkey = get_region_block_rkey(alloc_region, index); 
    if(alloc_region.base_map_ == bitmap32_filled) {
        update_section(alloc_region, alloc_exclusive);
    }
    return true;
}

bool RDMAConnection::fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive) {
    int index; region_e new_region;
    do {
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ == 0) {
            printf("already exclusive\n");
            return false;
        } 
        new_region = alloc_region;
        if((index = find_free_index_from_bitmap16_tail(alloc_region.class_map_)) == -1) {
            return false;
        }
        uint16_t mask = 0;
        for(int i = 0;i < block_class + 1;i++) {
            mask |= 1<<(index*(block_class + 1)+i);
        }
        new_region.base_map_ |= mask;
        new_region.class_map_ |= 1<<index;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(new_region.offset_), global_rkey_));
    alloc_region = new_region;
    addr = get_region_block_addr(alloc_region, index*(block_class + 1));
    rkey = get_region_class_block_rkey(alloc_region, index*(block_class + 1));
    return true;
}

bool RDMAConnection::free_region_block(uint64_t addr, bool is_exclusive) {
    uint32_t region_offset = (addr - heap_start_) / region_size_;
    uint32_t region_block_offset = (addr - heap_start_) % region_size_ / block_size_;
    region_e region;
    remote_read(&region, sizeof(region_e), region_metadata_addr(region_offset), global_rkey_);
    if(region.exclusive_ != is_exclusive) {
        printf("exclusive error, the actual exclusive bit is %d\n", region.exclusive_);
        return false;
    }
    if(region.base_map_>>region_block_offset % 2 == 0) {
        printf("already freed\n");
        return false;
    }
    if(region.block_class_ == 0) {

    } else {

    }
    return false;
}


}  // namespace kv
