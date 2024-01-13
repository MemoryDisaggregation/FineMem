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

    //struct ibv_qp_attr attr;
    //attr.max_dest_rd_atomic = 16;
    //attr.min_rnr_timer = 0x12;
    //attr.ah_attr.is_global = 1;
    //attr.ah_attr.sl    = 0;
    //attr.ah_attr.src_path_bits = 0;
   // attr.ah_attr.grh.flow_label = 0;
    //attr.ah_attr.grh.hop_limit  = 1;
    //attr.ah_attr.grh.traffic_class = 0;
    //attr.timeout = 0x12;
    //attr.retry_cnt = 6;
    //attr.rnr_retry = 0;
    //attr.sq_psn = 0;
    //attr.max_rd_atomic = 16;
    //int    attr_mask;
    //attr_mask = IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MAX_QP_RD_ATOMIC;
    //attr_mask = IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER
      //          |IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
        //        IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    //int ret = ibv_modify_qp(m_cm_id_->qp, &attr, attr_mask);
    //if(ret != 0) printf("connect error,%d\n",ret);

    uint8_t access_type_ = access_type;
    struct rdma_conn_param conn_param = {};
    conn_param.responder_resources = 16;
    conn_param.private_data = &access_type_;
    conn_param.private_data_len = sizeof(access_type_);
    conn_param.initiator_depth = 16;
    conn_param.retry_count = 6;
    //conn_param.flow_control = 0;
    //conn_param.rnr_retry_count = 0;
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
    section_class_header_ = (uint64_t)((section_e*)section_header_ + section_num_);
    region_header_ = (uint64_t)((section_class_e*)section_class_header_ + block_class_num*section_num_);
    block_rkey_ = (uint64_t)((region_e*)region_header_ + region_num_);
    class_block_rkey_ = (uint64_t)((uint32_t*)block_rkey_ + block_num_);
    block_header_ = (uint64_t)((uint32_t*)class_block_rkey_ + block_num_);
    backup_rkey_ = (uint64_t)((uint64_t*)block_header_ + block_num_);
    
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
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
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

int RDMAConnection::remote_rebind(uint64_t addr, uint32_t block_class, uint32_t &newkey){
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    RebindBlockRequest *request = (RebindBlockRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_MW_REBIND;
    request->addr = addr;
    request->block_class = block_class;
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

int RDMAConnection::remote_class_bind(uint32_t region_offset, uint16_t block_class){
    memset(m_cmd_msg_, 0, sizeof(CmdMsgBlock));
    memset(m_cmd_resp_, 0, sizeof(CmdMsgRespBlock));
    m_cmd_resp_->notify = NOTIFY_IDLE;
    ClassBindRequest *request = (ClassBindRequest *)m_cmd_msg_;
    request->resp_addr = (uint64_t)m_cmd_resp_;
    request->resp_rkey = m_resp_mr_->rkey;
    request->id = conn_id_;
    request->type = MSG_MW_CLASS_BIND;
    request->region_offset = region_offset;
    request->block_class = block_class;
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
  // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
  //  resp_msg->rkey);
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
    // printf("receive response: addr: %ld, key: %d\n", resp_msg->addr,
    //  resp_msg->rkey);
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

inline bool RDMAConnection::check_section(section_e alloc_section, alloc_advise advise, uint32_t offset) {
    switch (advise) {
    case alloc_empty:
        return ((~alloc_section.alloc_map_ & ~alloc_section.class_map_) & (bitmap32)1<< offset) != 0;
    case alloc_no_class:
        return ((alloc_section.alloc_map_ & ~alloc_section.class_map_) & (bitmap32)1<< offset) != 0;
    case alloc_class:
        return ((~alloc_section.alloc_map_ & alloc_section.class_map_) & (bitmap32)1<< offset) != 0;
    case alloc_exclusive:
        return ((alloc_section.alloc_map_ & alloc_section.class_map_) & (bitmap32)1<< offset) != 0;
    }
    return false;
}

bool RDMAConnection::update_section(uint32_t region_index, alloc_advise advise, alloc_advise compare) {
    uint64_t section_offset = region_index/region_per_section;
    uint64_t region_offset = region_index%region_per_section;
    section_e section_old;
    remote_read(&section_old, sizeof(section_old), section_metadata_addr(section_offset), global_rkey_);
    if(check_section(section_old, advise, region_offset)) {
        return true;
    }
    section_e section_new = section_old;
    if(advise == alloc_exclusive) {
        do{
            // if(!check_section(section_old, compare, region_offset)){
            //     // printf("try update_section failed, compare is %d, advise is %d, class bit is %d, malloc bit is %d\n", compare, advise,
            //     //     (section_old.class_map_ >> region_offset) % 2, (section_old.alloc_map_ >> region_offset) % 2);
            //     return false;
            // }
            section_new = section_old;
            section_new.alloc_map_ |= (bitmap32)1 << region_offset;
            section_new.class_map_ |= (bitmap32)1 << region_offset;
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_empty) {
        do{
            //if(!check_section(section_old, compare, region_offset)){
                // printf("try update_section failed, compare is %d, advise is %d, class bit is %d, malloc bit is %d\n", compare, advise,
                //     (section_old.class_map_ >> region_offset) % 2, (section_old.alloc_map_ >> region_offset) % 2);
              //  return false;
            //}
            section_new = section_old;
            section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
            section_new.class_map_ &= ~((bitmap32)1 << region_offset);
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_no_class) {
        do{
            //if(!check_section(section_old, compare, region_offset)){
                // printf("try update_section failed, compare is %d, advise is %d, class bit is %d, malloc bit is %d\n", compare, advise,
                //     (section_old.class_map_ >> region_offset) % 2, (section_old.alloc_map_ >> region_offset) % 2);
               // return false;
            //}
            section_new = section_old;
            section_new.class_map_ &= ~((bitmap32)1 << region_offset);
            section_new.alloc_map_ |= (bitmap32)1 << region_offset;
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    } else if(advise == alloc_class) {
        do{
            //if(!check_section(section_old, compare, region_offset)){
                // printf("try update_section failed, compare is %d, advise is %d, class bit is %d, malloc bit is %d\n", compare, advise,
                //     (section_old.class_map_ >> region_offset) % 2, (section_old.alloc_map_ >> region_offset) % 2);
               // return false;
            //}
            section_new = section_old;
            section_new.class_map_ |= (bitmap32)1 << region_offset;
            section_new.alloc_map_ &= ~((bitmap32)1 << region_offset);
        }while(!remote_CAS(*(uint64_t*)&section_new, (uint64_t*)&section_old, section_metadata_addr(section_offset), global_rkey_));
        return true;
    }
    return false;
}

bool RDMAConnection::find_section(uint16_t block_class, section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) {
    section_e section[8] = {0,0};
    if(advise == alloc_class) {
        int remain = section_num_, fetch = remain > 8 ? 8:remain, index = 0;
        section_class_e section_class_header = {0,0};
        while(remain > 0) {
            remote_read(section, fetch*sizeof(section_e), section_metadata_addr(index), global_rkey_);
            for(int j = 0; j < fetch; j ++) {
                if(free_bit_in_bitmap32(section[j].class_map_ | section[j].alloc_map_) > region_per_section/3){
                    alloc_section = section[j];
                    section_offset = index + j;
                    // printf("find new section from section\n");
                    return true;
                }
            }
            for(int j = 0; j < fetch; j ++) {
                if((section[j].class_map_ | section[j].alloc_map_) != ~(uint32_t)0){
                    alloc_section = section[j];
                    section_offset = index + j;
                    // printf("find new section from section\n");
                    return true;
                }
            }
            for(int j = 0; j < fetch; j ++) {
                uint32_t fast_index = get_section_class_index(index + j, block_class);
                remote_read(&section_class_header, sizeof(section_class_e), section_class_metadata_addr(fast_index), global_rkey_);
                if((section_class_header.class_map_ & section_class_header.alloc_map_) != ~(uint32_t)0){
                    alloc_section = section[j];
                    section_offset = index + j;
                    // printf("find new section from section class\n");
                    return true;
                }
            }
            index += 8; remain -= 8; fetch = remain > 8 ? 8:remain;
        }
    } else if(advise == alloc_no_class) {
        int remain = section_num_, fetch = remain > 8 ? 8:remain, index = 0;
        while(remain > 0) {
            remote_read(section, fetch*sizeof(section_e), section_metadata_addr(index), global_rkey_);
            for(int j = 0; j < fetch; j ++) {
                if(free_bit_in_bitmap32(section[j].class_map_ & section[j].alloc_map_) > region_per_section/3){
                    alloc_section = section[j];
                    section_offset = index + j;
                    return true;
                }
            }
            for(int j = 0; j < fetch; j ++) {
                if((section[j].class_map_ & section[j].alloc_map_) != ~(uint32_t)0){
                    alloc_section = section[j];
                    section_offset = index + j;
                    return true;
                }
            }
            index += 8; remain -= 8; fetch = remain > 8 ? 8:remain;
        }
    } else if (advise == alloc_empty) {
        int remain = section_num_, fetch = remain > 8 ? 8:remain, index = 0;
        while(remain > 0) {
            remote_read(section, fetch*sizeof(section_e), section_metadata_addr(index), global_rkey_);
            for(int j = 0; j < fetch; j ++) {
                if(free_bit_in_bitmap32(section[j].class_map_ | section[j].alloc_map_) > region_per_section/3){
                    alloc_section = section[j];
                    section_offset = index + j;
                    return true;
                }
            }
            for(int j = 0; j < fetch; j ++) {
                if((section[j].class_map_ | section[j].alloc_map_) != ~(uint32_t)0){
                    alloc_section = section[j];
                    section_offset = index + j;
                    return true;
                }
            }
            index += 8; remain -= 8; fetch = remain > 8 ? 8:remain;
        }
    } else { return false; }
    printf("find no section!\n");
    return false;
}

bool RDMAConnection::fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region, uint32_t &region_index) {
    if(block_class == 0 && shared == true) {
        // force use unclassed one to alloc single block
        section_e new_section;
        uint32_t free_map;
        int index;
        remote_read(&alloc_section, sizeof(section_e), section_metadata_addr(section_offset), global_rkey_);
        do {
            free_map = alloc_section.class_map_ & alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                // free_map = alloc_section.class_map_ & alloc_section.alloc_map_;
                // if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                    // printf("section has no free space!\n");
                    return false;
                // }
            }
            // if first alloc, do state update
            new_section = alloc_section;
            raise_bit(new_section.alloc_map_, new_section.class_map_, index);
            // if(((alloc_section.alloc_map_>>index) & 1) != 0) {
            //     new_section.class_map_ |= ((uint32_t)1<<index);
            //     // break;
            // }
            // new_section.alloc_map_ |= ((uint32_t)1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        region_index = section_offset*region_per_section+index;
        remote_read(&alloc_region, sizeof(region_e), region_metadata_addr(region_index), global_rkey_);
        return true;
    }
    else if(block_class == 0 && shared == false) {
        section_e new_section;
        uint32_t free_map;
        int index;
        do {
            free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                // printf("section has no free space!\n");
                return false;
            }
            new_section = alloc_section;
            new_section.alloc_map_ |= ((uint32_t)1<<index);
            new_section.class_map_ |= ((uint32_t)1<<index);
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
        }while(!remote_CAS(*(uint64_t*)&region_new, (uint64_t*)&region_old, region_metadata_addr(region_index), global_rkey_));
        region_old = region_new;
        alloc_region = region_old;
        return true;
    }
    // class alloc, shared
    else if (shared == true) {
        int index; bool has_free = true;
        uint32_t fast_index = get_section_class_index(section_offset, block_class);
        region_e region_new, region_old;
        section_class_e section_class_header;
        remote_read(&section_class_header, sizeof(section_class_e), section_class_metadata_addr(fast_index), global_rkey_);
        section_class_e new_section_class_header = section_class_header;
        uint32_t class_map = section_class_header.class_map_ & section_class_header.alloc_map_;
        do {
            new_section_class_header = section_class_header;
            if( (index = find_free_index_from_bitmap32_tail(class_map)) == -1 ){
                has_free = false;
                break;
            }
            raise_bit(new_section_class_header.alloc_map_, new_section_class_header.class_map_, index);
        } while(!remote_CAS(*(uint64_t*)&new_section_class_header, (uint64_t*)&section_class_header, section_class_metadata_addr(fast_index), global_rkey_));
        if(has_free) {
            remote_read(&alloc_region, sizeof(region_e), region_metadata_addr(section_offset * region_per_section + index), global_rkey_);
            return true;
        }
        uint32_t free_map;
        section_e new_section;
        do {
            free_map = alloc_section.class_map_ | alloc_section.alloc_map_;
            if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                // free_map = alloc_section.class_map_;
                // if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                //     printf("section has no free space!\n");
                    return false;
                // }
            }
            new_section = alloc_section;
            new_section.class_map_ |= ((uint32_t)1<<index);
            new_section.alloc_map_ |= ((uint32_t)1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        region_index = section_offset*region_per_section+index;
        remote_read(&region_old, sizeof(region_e), region_metadata_addr(region_index), global_rkey_);
        init_region_class(region_old, block_class, 0, region_index);
        remote_class_bind(region_index, block_class);
        do{
            new_section_class_header = section_class_header;
            if(new_section_class_header.alloc_map_ & new_section_class_header.class_map_ >> index != 1) {
                break;
            }
            new_section_class_header.alloc_map_ &= ~(uint32_t)(1<<index);
            new_section_class_header.class_map_ &= ~(uint32_t)(1<<index);
        } while (!remote_CAS(*(uint64_t*)&new_section_class_header, (uint64_t*)&section_class_header, section_class_metadata_addr(fast_index), global_rkey_));
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
            if( (index = find_free_index_from_bitmap32_tail(free_map)) == -1 ){
                // free_map = alloc_section.class_map_;
                // if( (index = find_free_index_from_bitmap32_lead(free_map)) == -1 ){
                    // printf("section has no free space!\n");
                    return false;
                // }
            }
            new_section = alloc_section;
            new_section.class_map_ |= ((uint32_t)1<<index);
            new_section.alloc_map_ |= ((uint32_t)1<<index);
        }while(!remote_CAS(*(uint64_t*)&new_section, (uint64_t*)&alloc_section, section_metadata_addr(section_offset), global_rkey_));
        alloc_section = new_section;
        region_index = section_offset*region_per_section+index;
        remote_read(&region_old, sizeof(region_e), region_metadata_addr(region_index), global_rkey_);
        init_region_class(region_old, block_class, 1, region_index);
        remote_class_bind(region_index, block_class);
        alloc_region = region_old;
        return true;
    }
    return false;
}

bool RDMAConnection::try_add_section_class(uint32_t section_offset, uint32_t block_class, uint32_t region_index){
    // bool recycle = false; 
    uint32_t fast_index = get_section_class_index(section_offset, block_class);
    uint32_t region_index_ = region_index % region_per_section;
    section_class_e old_section_class;
    remote_read(&old_section_class, sizeof(section_class_e), section_class_metadata_addr(fast_index), global_rkey_);
    section_class_e new_section_class = old_section_class;
    do{
        new_section_class = old_section_class;
        // if(new_section_class.alloc_map_ | new_section_class.class_map_ >> region_index % 2 == 0) {
        //     new_section_class.alloc_map_ |= 1<< region_index;
        //     new_section_class.class_map_ |= 1<< region_index;
        //     recycle = true;
        // } else
        down_bit(new_section_class.alloc_map_, new_section_class.class_map_, region_index_);
    } while(!remote_CAS(*(uint64_t*)&new_section_class, (uint64_t*)&old_section_class, section_class_metadata_addr(fast_index), global_rkey_));
    // if(recycle) {
    //     set_region_empty(alloc_region);
    // }
    // printf("free success\n");
    return true;
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

bool RDMAConnection::set_region_exclusive(region_e &alloc_region, uint32_t region_index) {
    if(!update_section(region_index, alloc_exclusive, alloc_empty)){
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
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    return true;
}

bool RDMAConnection::set_region_empty(region_e &alloc_region, uint32_t region_index) {
    if(alloc_region.exclusive_ == 1) {
        // if(alloc_region.exclusive_ != 1) {
        //     if(!set_region_exclusive(alloc_region))
        //         return false;
        // }
        region_e new_region;
        do {
            new_region = alloc_region;
            if(new_region.base_map_ != 0) {
                // printf("wait for free\n");
                return false;
            }
            new_region.exclusive_ = 0;
            new_region.block_class_ = 0;
        } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
        alloc_region = new_region;
        if(!update_section(region_index, alloc_empty, alloc_exclusive)){
            return false;
        }
    } else if(alloc_region.block_class_ == 0) {
        // if(alloc_region.exclusive_ != 1) {
        //     if(!set_region_exclusive(alloc_region))
        //         return false;
        // }
        region_e new_region;
        do {
            new_region = alloc_region;
            if(new_region.base_map_ != 0) {
                // printf("wait for free\n");
                return false;
            }
            new_region.exclusive_ = 0;
            new_region.block_class_ = 0;
        } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
        alloc_region = new_region;
        if(!update_section(region_index, alloc_empty, alloc_no_class)){
            return false;
        }
    } else {
        region_e new_region;
        do {
            new_region = alloc_region;
            if(free_bit_in_bitmap16(new_region.class_map_) != block_per_region/(new_region.block_class_+1)) {
                // printf("wait for free\n");
                return false;
            }
            new_region.exclusive_ = 0;
            new_region.block_class_ = 0;
        } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
        alloc_region = new_region;
        if(!update_section(region_index, alloc_empty, alloc_class)){
            return false;
        }
    }
    // printf("set empty success\n");
    return true;
}

bool RDMAConnection::init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive, uint32_t region_index) {
    // suppose the section has already set!
    region_e new_region;
    do {
        new_region = alloc_region;
        if((alloc_region.block_class_ != 0 && alloc_region.block_class_ != block_class))  {
            return false;
        } 
        uint16_t mask = 0;
        uint32_t reader = new_region.base_map_;
        uint32_t tail = (uint32_t)1<<(block_class+1);
        for(int i = 0; i < block_per_region/(block_class+1); i++ ) {
            if(reader%tail == 0)
                mask |= (uint16_t)1<<i;
            reader >>= block_class+1;
        }
        new_region.exclusive_ = is_exclusive;
        new_region.class_map_ = ~mask;
        new_region.block_class_ = block_class;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    return true;
}

int RDMAConnection::fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) {
    int index, retry_time = 0; region_e new_region;
    do{
	retry_time ++;
	//if(retry_time>10) {
	 // printf("retry time:%d\n",retry_time);
	//}
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ != 0) {
            printf("state wrong, addr = %lx, exclusive = %d, class = %u\n", get_region_addr(region_index), alloc_region.exclusive_, alloc_region.block_class_);
            return 0;
        } 
        new_region = alloc_region;
        if((index = find_free_index_from_bitmap32_tail(alloc_region.base_map_)) == -1) {
            // update_section(alloc_region, alloc_exclusive, alloc_no_class);
            return 0;
        }
        new_region.base_map_ |= (uint32_t)1<<index;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    addr = get_region_block_addr(region_index, index);
    rkey = get_region_block_rkey(region_index, index); 
    if(alloc_region.base_map_ == bitmap32_filled) {
        update_section(region_index, alloc_exclusive, alloc_no_class);
            // printf("try to set region as exclusive\n");
        // }
    }
    return retry_time;
}

int RDMAConnection::fetch_region_batch(region_e &alloc_region, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) {
    int index; region_e new_region;
    uint64_t free_item = 0;
    do{
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ != 0) {
            printf("state wrong, addr = %lx, exclusive = %d, class = %u\n", get_region_addr(region_index), alloc_region.exclusive_, alloc_region.block_class_);
            return 0;
        } 
        new_region = alloc_region;
        if((free_item = free_bit_in_bitmap32(alloc_region.base_map_)) == 0) {
            // printf("no free space in this region\n");
            // update_section(alloc_region, alloc_exclusive, alloc_no_class);
            return 0;
        }
        // printf("have free space\n");
        free_item = num < free_item ? num : free_item;
        for(int i = 0; i < free_item; i ++) {
            index = find_free_index_from_bitmap32_tail(new_region.base_map_);    
            new_region.base_map_ |= (uint32_t)1<<index;
            addr[i].addr = index;
        }
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    uint32_t rkey_list[block_per_region];
    fetch_exclusive_region_rkey(region_index, rkey_list);
    for(int i = 0; i < free_item; i++){
        addr[i].rkey = rkey_list[addr[i].addr]; 
        // addr[i].rkey = get_region_block_rkey(region_index, addr[i].addr); 
        addr[i].addr = get_region_block_addr(region_index, addr[i].addr);    
        // printf("get addr %p, %u\n", addr[i].addr, addr[i].rkey);
    }
    if(alloc_region.base_map_ == bitmap32_filled) {
        update_section(region_index, alloc_exclusive, alloc_no_class);
    }
    return free_item;
}

bool RDMAConnection::fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) {
    int index; region_e new_region;
    do {
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ != block_class) {
            printf("exclusive:%d, block_class:%d\n", alloc_region.exclusive_, alloc_region.block_class_);
            return false;
        } 
        new_region = alloc_region;
        if((index = find_free_index_from_bitmap16_tail(alloc_region.class_map_)) == -1) {
            // printf("already full\n");
            return false;
        }
        // printf("index = %d\n", index);
        uint32_t mask = 0;
        for(int i = 0;i < block_class + 1;i++) {
            mask |= (uint32_t)1<<(index*(block_class + 1)+i);
        }
        new_region.base_map_ |= mask;
        new_region.class_map_ |= (uint16_t)1<<index;
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    addr = get_region_block_addr(region_index, index*(block_class + 1));
    rkey = get_region_class_block_rkey(region_index, index*(block_class + 1));
    if(alloc_region.class_map_ == bitmap16_filled) {
        uint32_t fast_index = get_section_class_index(region_index/region_per_section, block_class);
        section_class_e section_class_header;
        remote_read(&section_class_header, sizeof(section_class_e), section_class_metadata_addr(fast_index), global_rkey_);
        section_class_e new_section_class_header = section_class_header;
        do {
            new_section_class_header = section_class_header;
            new_section_class_header.alloc_map_ |= 1 << region_index%region_per_section;
            new_section_class_header.class_map_ |= 1 << region_index%region_per_section;
        } while(!remote_CAS(*(uint64_t*)&new_section_class_header, (uint64_t*)&section_class_header, section_class_metadata_addr(fast_index), global_rkey_));
    }
    return true;
}

int RDMAConnection::fetch_region_class_batch(region_e &alloc_region, uint32_t block_class, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) {
    int index; region_e new_region;
    uint64_t free_item = 0;
    do{
        if(alloc_region.exclusive_ != is_exclusive || alloc_region.block_class_ != block_class) {
            printf("exclusive or block class not fit\n");
            return 0;
        } 
        new_region = alloc_region;
        if((free_item = free_bit_in_bitmap16(alloc_region.class_map_)) == 0) {
            // printf("no free block any more\n");
            return 0;
        }
        free_item = num < free_item ? num : free_item;
        for(int i = 0; i < free_item; i ++) {
            index = find_free_index_from_bitmap16_tail(new_region.class_map_);    
            uint32_t mask = 0;
            for(int j = 0;j < block_class + 1;j++) {
                mask |= (uint32_t)1<<(index*(block_class + 1)+j);
            }
            new_region.base_map_ |= mask;
            new_region.class_map_ |= (uint16_t)1<<index;
            addr[i].addr = index*(block_class + 1);
        }
    } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&alloc_region, region_metadata_addr(region_index), global_rkey_));
    alloc_region = new_region;
    uint32_t rkey_list[block_per_region];
    fetch_class_region_rkey(region_index, rkey_list);
    for(int i = 0; i < free_item; i++){
        addr[i].rkey = rkey_list[addr[i].addr];
        addr[i].addr = get_region_block_addr(region_index, addr[i].addr);    
    }
    if(alloc_region.class_map_ == bitmap16_filled) {
        uint32_t fast_index = get_section_class_index(region_index/region_per_section, block_class);
        section_class_e section_class_header;
        remote_read(&section_class_header, sizeof(section_class_e), section_class_metadata_addr(fast_index), global_rkey_);
        section_class_e new_section_class_header = section_class_header;
        uint32_t class_map = section_class_header.class_map_ & section_class_header.alloc_map_;
        do {
            new_section_class_header = section_class_header;
            new_section_class_header.alloc_map_ |= 1 << region_index%region_per_section;
            new_section_class_header.class_map_ |= 1 << region_index%region_per_section;
        } while(!remote_CAS(*(uint64_t*)&new_section_class_header, (uint64_t*)&section_class_header, section_class_metadata_addr(fast_index), global_rkey_));
    }
    return free_item;
}

int RDMAConnection::free_region_batch(uint32_t region_offset, uint32_t free_bitmap, bool is_exclusive) {
    region_e region;
    // printf("Remove me after no problem: the one-sided free not concern the section state\n");
    remote_read(&region, sizeof(region_e), region_metadata_addr(region_offset), global_rkey_);
    // printf("plan to free: %p, region id:%u, bitmap: %x\n", addr, region.offset_ , region.base_map_);

    if(!region.exclusive_ && is_exclusive) {
        printf("exclusive error, the actual block is shared\n");
        return -1;
    }
    uint32_t new_rkey;
    if(region.block_class_ == 0) {
        if((region.base_map_ & ~free_bitmap) == 0) {
            printf("already freed\n");
            return -1;
        } 
        // remote_rebind(addr, region.block_class_, new_rkey);
        region_e new_region;
        do{
            new_region = region;
            new_region.base_map_ &= free_bitmap;
        } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&region, region_metadata_addr(region_offset), global_rkey_));
        // printf("free: %p, region id:%u, bitmap from %x to %x\n", addr, new_region.offset_ , region.base_map_, new_region.base_map_);
        if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) == block_per_region){
            update_section(region_offset, alloc_empty, alloc_no_class); 
            return -2;
        } else if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) > 3*block_per_region/4 && free_bit_in_bitmap32(region.base_map_) <= 3*block_per_region/4){
            update_section(region_offset, alloc_no_class, alloc_class); 
        } else if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) > block_per_region/2 && free_bit_in_bitmap32(region.base_map_) <= block_per_region/2){
            update_section(region_offset, alloc_class, alloc_exclusive);
        } 
        region = new_region;
        return 0;
    } else {
        printf("class block batch free not support!\n");
    }
    return -1;
}

int RDMAConnection::free_region_block(uint64_t addr, bool is_exclusive) {
    uint32_t region_offset = (addr - heap_start_) / region_size_;
    uint32_t region_block_offset = (addr - heap_start_) % region_size_ / block_size_;
    region_e region;
    // printf("Remove me after no problem: the one-sided free not concern the section state\n");
    remote_read(&region, sizeof(region_e), region_metadata_addr(region_offset), global_rkey_);
    // printf("plan to free: %p, region id:%u, bitmap: %x\n", addr, region.offset_ , region.base_map_);

    if(!region.exclusive_ && is_exclusive) {
        printf("exclusive error, the actual block is shared\n");
        return -1;
    }
    uint32_t new_rkey;
    if(region.block_class_ == 0) {
        if((region.base_map_ & ((uint32_t)1<<region_block_offset)) == 0) {
            printf("already freed\n");
            return -1;
        } 
        // remote_rebind(addr, region.block_class_, new_rkey);
        region_e new_region;
        do{
            new_region = region;
            new_region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
        } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&region, region_metadata_addr(region_offset), global_rkey_));
        // printf("free: %p, region id:%u, bitmap from %x to %x\n", addr, new_region.offset_ , region.base_map_, new_region.base_map_);
        if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) == block_per_region){
            update_section(region_offset, alloc_empty, alloc_no_class); 
            return -2;
        } else if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) > block_per_region/2 && free_bit_in_bitmap32(region.base_map_) <= block_per_region/2){
            update_section(region_offset, alloc_no_class, alloc_class); 
        } else if(!is_exclusive && free_bit_in_bitmap32(new_region.base_map_) >= 15*block_per_region/16 && free_bit_in_bitmap32(region.base_map_) < 15*block_per_region/16){
            update_section(region_offset, alloc_class, alloc_exclusive);
        } 
        region = new_region;
        return 0;
    } else {
        region_e new_region;
        uint16_t block_class = region.block_class_;
        if( (region.class_map_ & ((uint16_t)1<<region_block_offset/(block_class+1))) == 0) {
            printf("already freed\n");
            return -1;
        }
        // remote_rebind(addr, region.block_class_, new_rkey);
        do{
            new_region = region;
            uint32_t mask = 0; 
            for(int i = 0;i < block_class + 1;i++) {
                mask |= (uint32_t)1<<(region_block_offset+i);
            }
            new_region.base_map_ &= ~mask;
            new_region.class_map_ &= ~(uint16_t)(1<<region_block_offset/(block_class+1));
        } while(!remote_CAS(*(uint64_t*)&new_region, (uint64_t*)&region, region_metadata_addr(region_offset), global_rkey_));
        // if(free_bit_in_bitmap16(new_region.class_map_) == block_per_region/(block_class+1)) {
        //     // printf("[Attention] try to add fast region %lx\n", addr);
        //     // if(!try_add_fast_region(region_offset/region_per_section, block_class, new_region)) {
        //         // printf("[Attention] try to clean region %lx\n", addr);
        //     set_region_empty(new_region);
        //     return -2-block_class;
        //     // }
        // } else 
        if(!is_exclusive && free_bit_in_bitmap16(new_region.class_map_) == block_per_region/(block_class+1)){
            try_add_section_class(region_offset/region_per_section, block_class, region_offset);
            return -2;
        } else if(!is_exclusive && free_bit_in_bitmap16(new_region.class_map_) > 3*block_per_region/(block_class+1)/4 && free_bit_in_bitmap16(region.class_map_) <= 3*block_per_region/(block_class+1)/4) {
            try_add_section_class(region_offset/region_per_section, block_class, region_offset);
        } else if(!is_exclusive && free_bit_in_bitmap16(new_region.class_map_) > 1*block_per_region/(block_class+1)/2 && free_bit_in_bitmap16(region.class_map_) <= 1*block_per_region/(block_class+1)/2) {
            try_add_section_class(region_offset/region_per_section, block_class, region_offset);
        } else if(!is_exclusive && free_bit_in_bitmap16(new_region.class_map_) > block_per_region/(block_class+1)/4 && free_bit_in_bitmap16(region.class_map_) <= block_per_region/(block_class+1)/4){
            // printf("[Attention] try to add fast region %lx\n", addr);
            try_add_section_class(region_offset/region_per_section, block_class, region_offset);
        }  
        region = new_region;
        // printf("free a class %u block %lx, newkey is %u\n", new_region.block_class_, addr, new_rkey);
        return block_class;
    }
    return -1;
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

bool RDMAConnection::fetch_block(uint16_t block_class, uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) {
    uint64_t block_headers[block_class + 1]; 
    bool restart = false;
    uint64_t old_header = 0, new_header = 1, hint = block_hint % block_num_;
    do{
        int recorder = -1;
        if(hint + block_class + 1 >= block_num_) {
            hint = 0;
            if(restart)
                return false;
            restart = true;
        }
        remote_read(block_headers, (block_class + 1) * sizeof(uint64_t), block_header_ + hint * sizeof(uint64_t), global_rkey_);
        for(int i = 0; i < block_class + 1; i++) {
            if(block_headers[i] != 0) {
                recorder = i;
            }
        }
        if(recorder == -1) {
            for(int i = 0; i < block_class + 1; i++) {
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

bool RDMAConnection::free_block(uint16_t block_class, uint64_t addr) {
    uint64_t old_header = 1, new_header = 0;
    uint64_t index = (addr - heap_start_) / block_size_;
    for(int i = 0; i < block_class + 1; i++) {
        old_header = 1; new_header = 0;
        if(!remote_CAS(*(uint64_t*)&new_header, (uint64_t*)&old_header, block_header_ + (index+i) * sizeof(uint64_t), global_rkey_)){
            return false;
        };
    }
    return true;
}

}  // namespace kv
