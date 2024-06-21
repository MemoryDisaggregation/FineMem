#include "rdma_conn.h"
#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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
    conn_param.rnr_retry_count = 7;
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
    flength_header_ = (uint64_t)((section_e*)section_header_ + section_num_);
    region_header_ = (uint64_t)((flength_e*)flength_header_ + section_num_);
    block_rkey_ = (uint64_t)((region_e*)region_header_ + region_num_);
    class_block_rkey_ = (uint64_t)((uint32_t*)block_rkey_ + block_num_);
    block_header_ = (uint64_t)((uint32_t*)class_block_rkey_ + block_num_);
    backup_rkey_ = (uint64_t)((uint64_t*)block_header_ + block_num_);
    
    heap_start_ = server_pdata.heap_start_;
    
    
    assert(server_pdata.size == sizeof(CmdMsgBlock));

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
            ret = 0;
            break;
        } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
            perror("CAS IBV_WC_WR_FLUSH_ERR");
            break;
        } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
            perror("CAS IBV_WC_RNR_RETRY_EXC_ERR");
            break;
        } else {
            perror("CAS ibv_poll_cq status error");
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
  return 0;
}

int RDMAConnection::unregister_remote_memory(uint64_t addr) {
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    UnregisterRequest *request = (UnregisterRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_UNREGISTER;
    request->addr = addr;
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
    UnregisterResponse *resp_msg = (UnregisterResponse *)m_cmd_resp_;
    if (resp_msg->status != RES_OK) {
        printf("register remote memory fail\n");
        return -1;
    }
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

int RDMAConnection::remote_rebind(uint64_t addr, uint32_t block_length, uint32_t &newkey){
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    RebindBlockRequest *request = (RebindBlockRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_MW_REBIND;
    request->addr = addr;
    request->block_length = block_length;
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
    RebindBlockResponse *resp_msg = (RebindBlockResponse *)m_cmd_resp_;
    if (resp_msg->status != RES_OK) {
        printf("mem window bind fail\n");
        return -1;
    }
    newkey = resp_msg->rkey;

    return 0;
}

int RDMAConnection::remote_rebind_batch(uint64_t *addr, uint32_t *newkey){
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    RebindBatchRequest *request = (RebindBatchRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_MW_BATCH;
    for(int i = 0; i < 32; i++)
        request->addr[i] = addr[i];
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
    RebindBatchResponse *resp_msg = (RebindBatchResponse *)m_cmd_resp_;
    if (resp_msg->status != RES_OK) {
        printf("mem window bind fail\n");
        return -1;
    }
    for(int i = 0; i < 32; i++) {
        newkey[i] = resp_msg->rkey[i];
    }
    return 0;
}

int RDMAConnection::remote_class_bind(uint32_t region_offset, uint16_t block_length){
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    ClassBindRequest *request = (ClassBindRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_MW_CLASS_BIND;
    request->region_offset = region_offset;
    request->block_length = block_length;
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
    ClassBindResponse *resp_msg = (ClassBindResponse *)m_cmd_resp_;
    if (resp_msg->status != RES_OK) {
        printf("mem window bind fail\n");
        return -1;
    }

    return 0;
}

int RDMAConnection::remote_memzero(uint64_t addr, uint64_t size) {
    struct ibv_sge sge;
    memset(m_reg_buf_, 0, size);
    sge.addr = (uintptr_t)m_reg_buf_;
    sge.length = size;
    sge.lkey = m_reg_buf_mr_->lkey; 

    struct ibv_send_wr send_wr = {};
    struct ibv_send_wr *bad_send_wr;
    send_wr.wr_id = 0;
    send_wr.num_sge = 1;
    send_wr.next = NULL;
    send_wr.opcode = IBV_WR_RDMA_WRITE;
    send_wr.sg_list = &sge;
    send_wr.send_flags = IBV_SEND_SIGNALED;
    send_wr.wr.rdma.remote_addr = addr;
    send_wr.wr.rdma.rkey = global_rkey_;
    if (ibv_post_send(m_cm_id_->qp, &send_wr, &bad_send_wr)) {
        perror("ibv_post_send fail");
        return -1;
    }

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
            ret = 0;
            break;
        } else if (IBV_WC_WR_FLUSH_ERR == wc.status) {
            perror("memzero IBV_WC_WR_FLUSH_ERR");
            break;
        } else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status) {
            perror("memzero IBV_WC_RNR_RETRY_EXC_ERR");
            break;
        } else {
            perror("memzero ibv_poll_cq status error");
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
  return 0;
}

int RDMAConnection::remote_free_block(uint64_t addr) {
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    FreeFastRequest *request = (FreeFastRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_FREE_FAST;
    request->addr = addr;
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
    if (resp_msg->status != RES_OK) {
        printf("fetch block block fail\n");
        return -1;
    }
    return 0;
}

int RDMAConnection::remote_print_alloc_info() {
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    FreeFastRequest *request = (FreeFastRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_PRINT_INFO;
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
    if (resp_msg->status != RES_OK) {
        printf("fetch block block fail\n");
        return -1;
    }
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
  return 0;
}

inline bool RDMAConnection::check_section(section_e alloc_section, alloc_advise advise, uint32_t offset) {
    switch (advise) {
    case alloc_empty:
        return ((~alloc_section.alloc_map_ & ~alloc_section.frag_map_) & (bitmap32)1<< offset) != 0;
    case alloc_light:
        return ((alloc_section.alloc_map_ & ~alloc_section.frag_map_) & (bitmap32)1<< offset) != 0;
    case alloc_heavy:
        return ((~alloc_section.alloc_map_ & alloc_section.frag_map_) & (bitmap32)1<< offset) != 0;
    case alloc_full:
        return ((alloc_section.alloc_map_ & alloc_section.frag_map_) & (bitmap32)1<< offset) != 0;
    }
    return false;
}

// each section state update is up-to-date and overwrite the old state
// update a state of alloc_empty is forbidden, only a fetch operation can do this
bool RDMAConnection::force_update_section_state(section_e &section, uint32_t region_index, alloc_advise advise) {
    uint64_t section_offset = region_index/region_per_section;
    uint64_t region_offset = region_index%region_per_section;
    section_e section_new;
    remote_read(&section, sizeof(section), section_metadata_addr(section_offset), global_rkey_);
    if(check_section(section, advise, region_offset)){
        return true;
    } else if (check_section(section, alloc_empty, region_offset)){
        // Error: from empty --> allocated
        return false;
    }
    if(advise == alloc_full) {
        do{
            section_new = section;
            section_new.alloc_map_ |= (bitmap32)1 << region_offset;
            section_new.frag_map_ |= (bitmap32)1 << region_offset;
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_empty) {
        do{
            section_new = section;
            section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
            section_new.frag_map_ &= ~((bitmap32)1 << region_offset);
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_light) {
        do{
            section_new = section;
            section_new.frag_map_ &= ~((bitmap32)1 << region_offset);
            section_new.alloc_map_ |= (bitmap32)1 << region_offset;
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_heavy) {
        do{
            section_new = section;
            section_new.frag_map_ |= (bitmap32)1 << region_offset;
            section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section, section_metadata_addr(section_offset), global_rkey_));
        return true;
    }
    return false;
}

bool RDMAConnection::force_update_flength_state(uint32_t region_index, uint8_t ratio) {
    uint64_t section_offset = region_index/region_per_section;
    uint64_t region_offset = region_index%region_per_section;
    flength_e alloc_flength, flength_new;
    remote_read(&alloc_flength, sizeof(alloc_flength), flength_metadata_addr(section_offset), global_rkey_);
    uint8_t ratio_old = (alloc_flength >> region_index*2)%4;
    if (ratio_old == ratio) {
        return true;
    }
    do{
        flength_new = alloc_flength;
        flength_new &= (bitmap64)ratio << (region_offset*2);
    }while(!remote_CAS(*(uint64_t*)&flength_new, (uint64_t*)&alloc_flength, flength_metadata_addr(section_offset), global_rkey_));
    return true;
}

// find a new section avaliable for an allocation with advise(usually alloc_full)
bool RDMAConnection::find_section(section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) {
    section_e section[8] = {0,0};
    int offset = section_offset%section_num_;
    // each epoch fetch 8 sections, 8*8B = 64Byte
    if (advise != alloc_empty) {
        int remain = section_num_, fetch = (offset + 8) > section_num_ ? (section_num_ - offset):8, index = offset;
        // if there are not fully alloc_full, this section can be used
        while(remain > 0) {
            remote_read(section, fetch*sizeof(section_e), section_metadata_addr(index), global_rkey_);
            for(int j = 0; j < fetch; j ++) {
                if((section[j].frag_map_ & section[j].alloc_map_) != ~(uint32_t)0){
                    alloc_section = section[j];
                    section_offset = index + j;
                    return true;
                }
            }
            index = (index + fetch)%section_num_; remain -= fetch; fetch = (index + 8) > section_num_ ? (section_num_ - index):8;
        }
    } else {
        int remain = section_num_, fetch = (offset + 8) > section_num_ ? (section_num_ - offset):8, index = offset;
        while(remain > 0) {
            // empty region exists
            remote_read(section, fetch*sizeof(section_e), section_metadata_addr(index), global_rkey_);
            for(int j = 0; j < fetch; j ++) {
                if((section[j].frag_map_ | section[j].alloc_map_) != ~(uint32_t)0){
                    alloc_section = section[j];
                    section_offset = index + j;
                    return true;
                }
            }
            index = (index + fetch)%section_num_; remain -= fetch; fetch = (index + 8) > section_num_ ? (section_num_ - index):8;
        }
    }
    printf("find no section!\n");
    return false;
}

// find an avalible region, exclusive, single, or varaint
bool RDMAConnection::fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_length, bool shared, region_e &alloc_region, uint32_t &region_index) {
    if(shared == true) {
        // both variant and single allocation with exclusive will fetch a full empty block and full use it
        section_e new_section;
        uint32_t free_map;
        int index;
        do {
            free_map = alloc_section.frag_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                return false;
            }
            new_section = alloc_section;
            new_section.alloc_map_ |= ((uint32_t)1<<index);
            new_section.frag_map_ |= ((uint32_t)1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        region_e region_new, region_old;
        remote_read(&region_old, sizeof(region_e), region_metadata_addr(section_offset*region_per_section+index), global_rkey_);
        region_index = section_offset*region_per_section+index;
        do {
            region_new = region_old;
            if(region_new.exclusive_ == 1) {
                printf("impossible problem: exclusive is already set\n");
                return false;
            }
            region_new.exclusive_ = 1;
            region_new.on_use_ = 1;
        }while(!remote_CAS(*(uint64_t*)&region_new, (uint64_t*)&region_old, region_metadata_addr(region_index), global_rkey_));
        region_old = region_new;
        alloc_region = region_old;
        return true;
    } else if(block_length == 1) {
        bool on_empty = false;
        section_e new_section;
        uint32_t empty_map, chance_map, normal_map;
        int index;
        // skip a read, only read at CAS failed
        // remote_read(&alloc_section, sizeof(section_e), section_metadata_addr(section_offset), global_rkey_);
        do {
            empty_map = alloc_section.frag_map_ | alloc_section.alloc_map_;
            chance_map = ~alloc_section.frag_map_ | alloc_section.alloc_map_;
            normal_map = alloc_section.frag_map_ | ~alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_tail(normal_map)) != -1 ){
                // no modify on map status
                new_section = alloc_section;
            } else if( (index = find_free_index_from_bitmap32_tail(chance_map)) != -1 ){
                // mark the chance map to full
                new_section = alloc_section;
                raise_bit(new_section.alloc_map_, new_section.frag_map_, index);
            } else if( (index = find_free_index_from_bitmap32_tail(empty_map)) != -1 ){
                // mark the empty map to allocated
                new_section = alloc_section;
                raise_bit(new_section.alloc_map_, new_section.frag_map_, index);    
                on_empty = true;
            } else {
                return false;
            }
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        region_e region_new;
        alloc_section = new_section;
        region_index = section_offset*region_per_section+index;
        // read region info
        remote_read(&alloc_region, sizeof(region_e), region_metadata_addr(region_index), global_rkey_);
        if (on_empty) {
            do {
                region_new = alloc_region;
                if(region_new.on_use_ == 1) {
                    printf("impossible problem: on_use is already set\n");
                    return false;
                }
                region_new.on_use_ = 1;
            }while(!remote_CAS(*(uint64_t*)&region_new, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
        }
        return true;
    }
    // variant length allocation with a shared region
    // the region must have long enough avaliable free-chunk to alloc
    // this region should be used immediately at variant allocation
    // or this function should not be called if no follwoing allocation
    // 00: 16-32, full empty
    // 01: 8-16, at least 16 avalible
    // 10: 1-8, at least 8 avaliable
    // 11: no length
    else {
        bool on_empty = false;
        section_e new_section;
        uint32_t empty_map, chance_map, normal_map;
        int index;
        // skip a read, only read at CAS failed
        remote_read(&alloc_section, sizeof(section_e), section_metadata_addr(section_offset), global_rkey_);
        do {
            empty_map = alloc_section.frag_map_ | alloc_section.alloc_map_;
            chance_map = ~alloc_section.frag_map_ | alloc_section.alloc_map_;
            normal_map = alloc_section.frag_map_ | ~alloc_section.alloc_map_;
            if( block_length < 16 && (index = find_free_index_from_bitmap32_tail(normal_map)) != -1 ){
                // no modify on map status
                new_section = alloc_section;
            } else if( block_length < 8 &&  (index = find_free_index_from_bitmap32_tail(chance_map)) != -1 ){
                // mark the chance map to full
                new_section = alloc_section;
                raise_bit(new_section.alloc_map_, new_section.frag_map_, index);
            } else if( (index = find_free_index_from_bitmap32_tail(empty_map)) != -1 ){
                // mark the empty map to allocated
                new_section = alloc_section;
                raise_bit(new_section.alloc_map_, new_section.frag_map_, index);    
                on_empty = true;
            } else {
                return false;
            }
        } while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        region_e region_new;
        alloc_section = new_section;
        region_index = section_offset*region_per_section+index;
        // read region info
        remote_read(&alloc_region, sizeof(region_e), region_metadata_addr(region_index), global_rkey_);
        if (on_empty) {
            do {
                region_new = alloc_region;
                if(region_new.on_use_ == 1) {
                    printf("impossible problem: on_use is already set\n");
                    return false;
                }
                region_new.on_use_ = 1;
            }while(!remote_CAS(*(uint64_t*)&region_new, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
        }
        return true;
    }
    return false;
}

// fetch varaint free regions in a section
bool RDMAConnection::fetch_varaint_regions(section_e &alloc_section, uint32_t section_offset, uint64_t region_length, uint64_t &addr) {
    bitmap32 free_map = alloc_section.alloc_map_ | alloc_section.frag_map_;
    // Double check whether there is a long enough free space in this section
    if(max_longbit(free_map) < region_length) {
        remote_read((uint64_t*)&alloc_section, sizeof(section_e), section_metadata_addr(section_offset), global_rkey_);
        free_map = alloc_section.alloc_map_ | alloc_section.frag_map_;
        if(max_longbit(free_map) < region_length) {
            return false;
        }
    }
    int free_length = 0;
    // each section has 32 items
    for(int i = 0; i < 32; i++) {
        // a free space
        if(free_map%2 == 0) {
            free_length += 1;
            // length enough
            if(free_length == region_length) {
                section_e section_new = alloc_section;
                bitmap32 mask = 0;
                for(int j = i-free_length+1; j <= i; j++) {
                    mask |= 1 << j;
                }
                section_new.alloc_map_ |= mask;
                section_new.frag_map_ |= mask;
                // find the section header changed
                if(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_)){
                    i = 0; free_length = 0; free_map = alloc_section.alloc_map_ | alloc_section.frag_map_;
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

// Why region need an exclusive bit?
// Because someone can incorrectly use an exclusive region after a region is set as empty but a user not known and further use this region info
// To immediately find its exclusive, this state will be marked in the region info, importantly
bool RDMAConnection::force_update_region_state(region_e &alloc_region, uint32_t region_index, bool is_exclusive, bool on_use) {
    region_e new_region;
    do {
        new_region = alloc_region;
        if(new_region.exclusive_ == is_exclusive) {
            printf("impossible situation: exclusive has already been set\n");
            return false;
        }
        new_region.on_use_ = on_use;
        new_region.exclusive_ = is_exclusive;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    return true;
}

// the core function of fetch a single block from a region
// the user must check the state if CAS failed, and must update state if some condition occurs
int RDMAConnection::fetch_region_block(section_e &alloc_section, region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) {
    int index, retry_time = 0; region_e new_region;
    uint8_t old_length, new_length;
    do{
	
        retry_time++;
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.on_use_ != 1) {
            printf("Region not avaliable, addr = %lx, exclusive = %d, free_length = %u\n", get_region_addr(region_index), alloc_region.exclusive_, alloc_region.max_length_);
            // this fail will return to fetch a new section
            return 0;
        } 
        new_region = alloc_region;
        if((index = find_free_index_from_bitmap32_tail(alloc_region.base_map_)) == -1) {
            return 0;
        }
        new_region.base_map_ |= (uint32_t)1<<index;

        // update the max length info
        old_length = new_region.max_length_;
        new_length = max_longbit(new_region.base_map_);
        new_region.max_length_ = new_length;
    
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    
    alloc_region = new_region;
    addr = get_region_block_addr(region_index, index);
    rkey = get_region_block_rkey(region_index, index); 

    // retry counter for least 3 time allocation
    uint16_t old_retry = (retry_counter_.retry_num[0] + retry_counter_.retry_num[1] + retry_counter_.retry_num[2]) / 3;
    retry_counter_.retry_num[retry_counter_.retry_iter] = retry_time;
    retry_counter_.retry_iter = (retry_counter_.retry_iter + 1) % 3;
    uint16_t avg_retry = (retry_counter_.retry_num[0] + retry_counter_.retry_num[1] + retry_counter_.retry_num[2]) / 3;
    
    // concurrency state update, will async it in the future
    if(alloc_region.base_map_ == bitmap32_filled) {
        force_update_section_state(alloc_section, region_index, alloc_full);
    } else if(old_retry < 10 && avg_retry >= 10) {
        force_update_section_state(alloc_section, region_index, alloc_full);
    } else if(old_retry < 3 && avg_retry >= 3) {
        force_update_section_state(alloc_section, region_index, alloc_heavy);
    } else if(old_retry >= 3 && avg_retry < 3) {
        force_update_section_state(alloc_section, region_index, alloc_light);
    } else if(old_retry >= 10 && avg_retry < 10) {
        force_update_section_state(alloc_section, region_index, alloc_heavy);
    }
    
    // // length state update, will async it in the future
    // if(old_length >= 8 && new_length < 8 ) {
    //     force_update_flength_state(region_index, 3);
    // } else if(old_length >= 16 && new_length < 16) {
    //     force_update_flength_state(region_index, 2);
    // } else if(old_length >= 24 && new_length < 24) {
    //     force_update_flength_state(region_index, 1);
    // }
    
    return retry_time;
}

int RDMAConnection::fetch_region_batch(section_e &alloc_section, region_e &alloc_region, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) {
    int index; region_e new_region;
    uint8_t old_length, new_length;
    uint64_t free_item = 0;
    do{
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.on_use_ != 1) {
            printf("Region not avaliable, addr = %lx, exclusive = %d, free_length = %u\n", get_region_addr(region_index), alloc_region.exclusive_, alloc_region.max_length_);
            return 0;
        } 
        new_region = alloc_region;
        if((free_item = free_bit_in_bitmap32(alloc_region.base_map_)) == 0) {
            return 0;
        }
        free_item = num < free_item ? num : free_item;
        for(int i = 0; i < free_item; i ++) {
            index = find_free_index_from_bitmap32_tail(new_region.base_map_);    
            new_region.base_map_ |= (uint32_t)1<<index;
            addr[i].addr = index;
        }
        
        // update the max length info
        old_length = new_region.max_length_;
        new_length = max_longbit(new_region.base_map_);
        new_region.max_length_ = new_length;

    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    uint32_t rkey_list[block_per_region];
    fetch_exclusive_region_rkey(region_index, rkey_list);
    for(int i = 0; i < free_item; i++){
        addr[i].rkey = rkey_list[addr[i].addr]; 
        addr[i].addr = get_region_block_addr(region_index, addr[i].addr);    
    }

    // batch not see CAS competition as a critical problem
    if(alloc_region.base_map_ == bitmap32_filled) {
        force_update_section_state(alloc_section, region_index, alloc_full);
    }

    // length state update, will async it in the future
    if(old_length >= 8 && new_length < 8 ) {
        force_update_flength_state(region_index, 3);
    } else if(old_length >= 16 && new_length < 16) {
        force_update_flength_state(region_index, 2);
    } else if(old_length >= 24 && new_length < 24) {
        force_update_flength_state(region_index, 1);
    }

    return free_item;
}

int RDMAConnection::find_flength(flength_e &alloc_flength, section_e &alloc_section, uint32_t &section_offset, uint32_t block_length) {
    flength_e flength[8];
    section_e section[8];
    int offset = section_offset%section_num_;
    int remain = section_num_, fetch = (offset + 8) > section_num_ ? (section_num_ - offset):8, index = offset;
    while(remain > 0) {
        remote_read(section, fetch*sizeof(section_e), section_metadata_addr(index), global_rkey_);
        for(int j = 0; j < fetch; j ++) {
            for(int k = 0; k < 32; k++) {
                if(flength[j]%4 == (2-(block_length-1)/8)){
                    alloc_section = section[j];
                    section_offset = offset + j;
                    return true;
                }
            }
        }
        index = (index + fetch)%section_num_; remain -= fetch; fetch = (index + 8) > section_num_ ? (section_num_ - index):8;
    }
}

int RDMAConnection::fetch_region_variant(flength_e &alloc_flength, section_e &alloc_section, uint32_t section_offset, uint32_t block_length, region_e &alloc_region, uint32_t &region_index){

}

int RDMAConnection::fetch_region_variant_blocks(uint32_t block_length, uint64_t &addr, uint32_t &rkey) {
    int index; region_e new_region;
    flength_e alloc_flength;
    section_e alloc_section; uint32_t section_offset = 0;
    region_e alloc_region; uint32_t region_index;
    uint8_t old_length, new_length;
    find_flength(alloc_flength, alloc_section, block_length, section_offset);
    fetch_region(alloc_section, section_offset, block_length, true, alloc_region, region_index);
    while(alloc_region.exclusive_ != 0 || alloc_region.on_use_ != 1 || alloc_region.max_length_ < block_length) {
        while(!fetch_region(alloc_section, section_offset, block_length, true, alloc_region, region_index)){
            while(!find_flength(alloc_flength, alloc_section, block_length, section_offset)){
                printf("no avaliable flength\n");
            }
        }
    }

    int free_length = 0;
    bitmap32 free_map = alloc_region.base_map_;
    // each section has 32 items
    for(int i = 0; i < 32; i++) {
        // a free space
        if(free_map%2 == 0) {
            free_length += 1;
            // length enough
            if(free_length == block_length) {
                new_region = alloc_region;
                bitmap32 mask = 0;
                index = i-free_length+1;
                for(int j = i-free_length+1; j <= i; j++) {
                    mask |= 1 << j;
                }
                new_region.base_map_ |= mask;
                // find the section header changed
                // update the max length info
                old_length = new_region.max_length_;
                new_length = max_longbit(new_region.base_map_);
                new_region.max_length_ = new_length;
                alloc_region = new_region;
                if(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_)){
                    i = 0; free_length = 0; free_map = alloc_region.base_map_;
                    continue;
                }

                break;
            }
        } else {
            free_length = 0;
        }
        free_map >>= 1;
    } 
    addr = get_region_block_addr(region_index, index);
    rkey = get_region_block_rkey(region_index, index);
    
    // batch not see CAS competition as a critical problem
    if(alloc_region.base_map_ == bitmap32_filled) {
        force_update_section_state(alloc_section, region_index, alloc_full);
    }

    // length state update, will async it in the future
    if(old_length >= 8 && new_length < 8 ) {
        force_update_flength_state(region_index, 3);
    } else if(old_length >= 16 && new_length < 16) {
        force_update_flength_state(region_index, 2);
    } else if(old_length >= 24 && new_length < 24) {
        force_update_flength_state(region_index, 1);
    }

    return true;
}

int RDMAConnection::free_region_batch(uint32_t region_offset, uint32_t free_bitmap, bool is_exclusive) {
    region_e region, new_region;
    int retry_time = 0;
    remote_read(&region, sizeof(region_e), region_metadata_addr(region_offset), global_rkey_);

    if(!region.exclusive_ && is_exclusive) {
        printf("exclusive error, the actual block is shared\n");
        return -1;
    }
    uint32_t new_rkey;
    if((region.base_map_ & ~free_bitmap) == 0) {
        printf("already freed\n");
        return -1;
    } 
    do{
        retry_time++;
        new_region = region;
        new_region.base_map_ &= free_bitmap;
        if ( new_region.base_map_ == (bitmap32)0 ) {
            new_region.on_use_ = 0;
            new_region.exclusive_ = 0;
        }
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&region, region_metadata_addr(region_offset), global_rkey_));

    // retry counter for least 3 time allocation
    uint16_t old_retry = (retry_counter_.retry_num[0] + retry_counter_.retry_num[1] + retry_counter_.retry_num[2]) / 3;
    retry_counter_.retry_num[retry_counter_.retry_iter] = retry_time;
    retry_counter_.retry_iter = (retry_counter_.retry_iter + 1) % 3;
    uint16_t avg_retry = (retry_counter_.retry_num[0] + retry_counter_.retry_num[1] + retry_counter_.retry_num[2]) / 3;
    
    // concurrency state update, will async it in the future
    section_e alloc_section;
    if(!is_exclusive && new_region.base_map_ == (bitmap32)0 ){
        force_update_section_state(alloc_section, region_offset, alloc_empty); 
        return -2;
    } else if(old_retry < 10 && avg_retry >= 10) {
        force_update_section_state(alloc_section, region_offset, alloc_full);
    } else if(old_retry < 3 && avg_retry >= 3) {
        force_update_section_state(alloc_section, region_offset, alloc_heavy);
    } else if(old_retry >= 3 && avg_retry < 3) {
        force_update_section_state(alloc_section, region_offset, alloc_light);
    } else if(old_retry >= 10 && avg_retry < 10) {
        force_update_section_state(alloc_section, region_offset, alloc_heavy);
    }

    region = new_region;
    return 0;
}

int RDMAConnection::free_region_block(uint64_t addr, bool is_exclusive) {
    uint32_t region_offset = (addr - heap_start_) / region_size_;
    uint32_t region_block_offset = (addr - heap_start_) % region_size_ / block_size_;
    region_e region, new_region;
    int retry_time = 0;
    remote_read(&region, sizeof(region_e), region_metadata_addr(region_offset), global_rkey_);

    if(!region.exclusive_ && is_exclusive) {
        printf("exclusive error, the actual block is shared\n");
        return -1;
    }
    uint32_t new_rkey;
    if((region.base_map_ & ((uint32_t)1<<region_block_offset)) == 0) {
        printf("already freed\n");
        return -1;
    } 
    do{
        retry_time++;
        new_region = region;
        new_region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
        if ( new_region.base_map_ == (bitmap32)0 ) {
            new_region.on_use_ = 0;
            new_region.exclusive_ = 0;
        }
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&region, region_metadata_addr(region_offset), global_rkey_));

    // retry counter for least 3 time allocation
    uint16_t old_retry = (retry_counter_.retry_num[0] + retry_counter_.retry_num[1] + retry_counter_.retry_num[2]) / 3;
    retry_counter_.retry_num[retry_counter_.retry_iter] = retry_time;
    retry_counter_.retry_iter = (retry_counter_.retry_iter + 1) % 3;
    uint16_t avg_retry = (retry_counter_.retry_num[0] + retry_counter_.retry_num[1] + retry_counter_.retry_num[2]) / 3;
    
    // concurrency state update, will async it in the future
    section_e alloc_section;
    if(!is_exclusive && new_region.base_map_ == (bitmap32)0 ){
        force_update_section_state(alloc_section, region_offset, alloc_empty); 
        return -2;
    } else if(old_retry < 10 && avg_retry >= 10) {
        force_update_section_state(alloc_section, region_offset, alloc_full);
    } else if(old_retry < 3 && avg_retry >= 3) {
        force_update_section_state(alloc_section, region_offset, alloc_heavy);
    } else if(old_retry >= 3 && avg_retry < 3) {
        force_update_section_state(alloc_section, region_offset, alloc_light);
    } else if(old_retry >= 10 && avg_retry < 10) {
        force_update_section_state(alloc_section, region_offset, alloc_heavy);
    }

    region = new_region;
    return 0;
}

int RDMAConnection::fetch_block(uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) {
    uint64_t old_header = 0, new_header = 1, hint = block_hint % block_num_;
    uint16_t counter = 0;
    int retry_time = 1;
    while(!remote_CAS(*(uint64_t*)&new_header, (uint64_t*)&old_header, block_header_ + hint * sizeof(uint64_t), global_rkey_)){
        retry_time ++;
        hint = (hint + 1) % block_num_;
        old_header = 0; new_header = 1;
        if(hint == block_hint) {
            counter ++;
            if(counter >3) {
                return 0;
            }
        }
    };
    addr = get_block_addr(hint);
    rkey = get_block_rkey(hint);
    block_hint = (hint + 1) % block_num_;
    return retry_time;
}

bool RDMAConnection::fetch_block(uint16_t block_length, uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) {
    uint64_t block_headers[block_length + 1]; 
    bool restart = false;
    uint64_t old_header = 0, new_header = 1, hint = block_hint % block_num_;
    do{
        int recorder = -1;
        if(hint + block_length>= block_num_) {
            hint = 0;
            if(restart)
                return false;
            restart = true;
        }
        remote_read(block_headers, (block_length) * sizeof(uint64_t), block_header_ + hint * sizeof(uint64_t), global_rkey_);
        for(int i = 0; i < block_length; i++) {
            if(block_headers[i] != 0) {
                recorder = i;
            }
        }
        if(recorder == -1) {
            for(int i = 0; i < block_length; i++) {
                new_header = 1; old_header = 0;
                if(!remote_CAS(*(uint64_t*)&new_header, (uint64_t*)&old_header, block_header_ + (hint+i) * sizeof(uint64_t), global_rkey_)){
                    recorder = i;
                    break;
                }      
            }
            if(recorder != -1) {
                for(int i = 0; i < recorder; i ++) {
                    new_header = 0; old_header = 1;
                    if(!remote_CAS(*(uint64_t*)&new_header, (uint64_t*)&old_header, block_header_ + (hint+i) * sizeof(uint64_t), global_rkey_)){
                        printf("roll back failed\n");
                        return false;
                    }
                }
            } else {
                addr = get_block_addr(hint);
                rkey = get_block_rkey(hint);
                block_hint = hint + block_num_ + 1;
                return true;
            }
        }
        hint = hint + recorder + 1;
    } while(!(restart && hint >= block_hint));
    block_hint = hint;
    return false;
}

bool RDMAConnection::free_block(uint64_t addr) {
    uint64_t old_header = 1, new_header = 0;
    uint64_t index = (addr - heap_start_) / block_size_;
    if(!remote_CAS(*(uint64_t*)&new_header, (uint64_t*)&old_header, block_header_ + index * sizeof(uint64_t), global_rkey_)){
        return false;
    };
    return true;
}

bool RDMAConnection::free_block(uint16_t block_length, uint64_t addr) {
    uint64_t old_header = 1, new_header = 0;
    uint64_t index = (addr - heap_start_) / block_size_;
    for(int i = 0; i < block_length; i++) {
        old_header = 1; new_header = 0;
        if(!remote_CAS(*(uint64_t*)&new_header, (uint64_t*)&old_header, block_header_ + (index+i) * sizeof(uint64_t), global_rkey_)){
            return false;
        };
    }
    return true;
}

}  // namespace kv
