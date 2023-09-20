/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:26
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-15 16:43:55
 * @FilePath: /rmalloc_newbase/include/msg.h
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <assert.h>
#include <bits/stdint-uintn.h>
#include <stdint.h>
#include <chrono>

namespace mralloc {

#define NOTIFY_WORK 0xFF
#define NOTIFY_IDLE 0x00
#define MAX_MSG_SIZE 64
#define MAX_SERVER_WORKER 4
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s
#define MAX_REMOTE_SIZE (1UL << 25)

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END)                                      \
  (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)) \
       .count())

enum MsgType { MSG_REGISTER, MSG_UNREGISTER, MSG_FETCH, MSG_FETCH_FAST, MSG_MW_BIND, RPC_FUSEE_SUBTABLE };

enum ResStatus { RES_OK, RES_FAIL };

enum ConnMethod {CONN_RPC, CONN_ONESIDE, CONN_FUSEE};

#define CHECK_RDMA_MSG_SIZE(T) \
  static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")

struct PData {
  uint64_t buf_addr;
  uint32_t buf_rkey;
  uint32_t size;
  uint32_t init_rkey;
};

struct CmdMsgBlock {
  uint8_t rsvd1[MAX_MSG_SIZE - 1];
  volatile uint8_t notify;
};

struct CmdMsgRespBlock {
  uint8_t rsvd1[MAX_MSG_SIZE - 1];
  volatile uint8_t notify;
};

class RequestsMsg {
 public:
  uint64_t resp_addr;
  uint32_t resp_rkey;
  uint8_t type;
};
CHECK_RDMA_MSG_SIZE(RequestsMsg);

class ResponseMsg {
 public:
  uint8_t status;
};
CHECK_RDMA_MSG_SIZE(ResponseMsg);

class FetchFastResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
  uint32_t size;
};

class RegisterRequest : public RequestsMsg {
 public:
  uint64_t size;
};
CHECK_RDMA_MSG_SIZE(RegisterRequest);

class RegisterResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(RegisterResponse);

class MWbindRequest : public RequestsMsg {
 public:
  uint64_t newkey;
  uint64_t addr;
  uint32_t rkey;
  uint32_t size;
};
CHECK_RDMA_MSG_SIZE(MWbindRequest);

class MWbindResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
  uint64_t size;
};
CHECK_RDMA_MSG_SIZE(MWbindRequest);

class FetchRequest : public RequestsMsg {
 public:
  uint32_t size;
};
CHECK_RDMA_MSG_SIZE(MWbindRequest);

class FetchResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
  uint64_t size;
};
CHECK_RDMA_MSG_SIZE(MWbindRequest);

class FuseeSubtableResponse : public ResponseMsg {
 public:
  uint64_t addr;
  uint32_t rkey;
};

struct UnregisterRequest : public RequestsMsg {
 public:
  uint64_t addr;
};
CHECK_RDMA_MSG_SIZE(UnregisterRequest);

struct UnregisterResponse : public ResponseMsg {};
CHECK_RDMA_MSG_SIZE(UnregisterResponse);

}  // namespace kv