
#pragma once

#include <assert.h>
#include <bits/stdint-uintn.h>
#include <stdint.h>
#include <chrono>
#include <pthread.h>

namespace mralloc {

#define NOTIFY_WORK 0xFF
#define NOTIFY_IDLE 0x00
#define MAX_MSG_SIZE 512
#define MAX_SERVER_WORKER 1
#define MAX_SERVER_CLIENT 1024
#define RESOLVE_TIMEOUT_MS 5000
#define RDMA_TIMEOUT_US 10000000  // 10s
#define MAX_REMOTE_SIZE (1UL << 25)

#define TIME_NOW (std::chrono::high_resolution_clock::now())
#define TIME_DURATION_US(START, END)                                      \
  (std::chrono::duration_cast<std::chrono::microseconds>((END) - (START)) \
       .count())

enum MsgType { MSG_REGISTER, MSG_UNREGISTER, MSG_FETCH, MSG_FETCH_FAST, MSG_MW_BIND, RPC_FUSEE_SUBTABLE, MSG_MW_REBIND, MSG_MW_CLASS_BIND, MSG_FREE_FAST, MSG_PRINT_INFO, MSG_MW_BATCH};

enum ResStatus { RES_OK, RES_FAIL };

enum ConnMethod {CONN_RPC, CONN_ONESIDE, CONN_FUSEE};

enum MRType:uint8_t {MR_IDLE, MR_FREE, MR_TRANSFER};

#define CHECK_RDMA_MSG_SIZE(T) \
    static_assert(sizeof(T) < MAX_MSG_SIZE, #T " msg size is too big!")

// buffer size = 64 MiB
struct MsgBuffer {
    MRType msg_type[8][8];
    void* buffer;
};

struct PublicInfo {
    uint16_t pid_alive[1024];
    MsgBuffer node_buffer[128];
};

struct CNodeInit {
    uint16_t pid;
    uint8_t access_type;
};

struct PData {
    uint64_t buf_addr;
    uint32_t buf_rkey;
    uint64_t size;
    uint16_t id;
    uint64_t block_size_;
    uint64_t block_num_;
    uint32_t global_rkey_;
    uint64_t section_header_;
    uint64_t heap_start_;
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
    uint16_t id;
    uint8_t type;
};
CHECK_RDMA_MSG_SIZE(RequestsMsg);

class ResponseMsg {
public:
    uint8_t status;
};
CHECK_RDMA_MSG_SIZE(ResponseMsg);

class FetchBlockResponse : public ResponseMsg {
public:
    uint64_t addr;
    uint32_t rkey;
    uint64_t size;
};

class FreeFastRequest : public RequestsMsg{
public:
    uint64_t addr;
};
CHECK_RDMA_MSG_SIZE(FreeFastRequest);

class RebindBlockRequest : public RequestsMsg{
public:
    uint64_t addr;
};
CHECK_RDMA_MSG_SIZE(RebindBlockRequest);

class RebindBlockResponse : public ResponseMsg {
public:
    uint32_t rkey;
};
CHECK_RDMA_MSG_SIZE(RebindBlockResponse);

class RebindBatchRequest : public RequestsMsg{
public:
    uint64_t addr[32];
};
CHECK_RDMA_MSG_SIZE(RebindBlockRequest);

class RebindBatchResponse : public ResponseMsg {
public:
    uint32_t rkey[32];
};
CHECK_RDMA_MSG_SIZE(RebindBlockResponse);

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
    uint64_t size;
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
    uint64_t size;
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

typedef enum mi_malloc_kind_e {
    MI_MALLOC_NORMAL,
    MI_MALLOC_SHADOW
} mi_malloc_kind_t;

// thread id's
typedef size_t     mi_threadid_t;

typedef union mi_page_flags_s {
  uint8_t full_aligned;
  struct {
    uint8_t in_full : 1;
    uint8_t has_aligned : 1;
  } x;
} mi_page_flags_t;

typedef uintptr_t mi_thread_free_t;

typedef enum mi_page_kind_e {
  MI_PAGE_SMALL,    // small blocks go into 64KiB pages inside a segment
  MI_PAGE_MEDIUM,   // medium blocks go into 512KiB pages inside a segment
  MI_PAGE_LARGE,    // larger blocks go into a single page spanning a whole segment
  MI_PAGE_HUGE      // huge blocks (>512KiB) are put into a single page in a segment of the exact size (but still 2MiB aligned)
} mi_page_kind_t;

typedef struct mi_page_s {
  // "owned" by the segment
  uint8_t               segment_idx;       // index in the segment `pages` array, `page == &segment->pages[page->segment_idx]`
  uint8_t               segment_in_use:1;  // `true` if the segment allocated this page
  uint8_t               is_committed:1;    // `true` if the page virtual memory is committed
  uint8_t               is_zero_init:1;    // `true` if the page was initially zero initialized

  // layout like this to optimize access in `mi_malloc` and `mi_free`
  uint16_t              capacity;          // number of blocks committed, must be the first field, see `segment.c:page_clear`
  uint16_t              reserved;          // number of blocks reserved in memory
  mi_page_flags_t       flags;             // `in_full` and `has_aligned` flags (8 bits)
  uint8_t               free_is_zero:1;    // `true` if the blocks in the free list are zero initialized
  uint8_t               retire_expire:7;   // expiration count for retired blocks

  void*           free;              // list of available free blocks (`malloc` allocates from this list)
  uint32_t              used;              // number of blocks in use (including blocks in `local_free` and `thread_free`)
  uint32_t              xblock_size;       // size available in each block (always `>0`)
  void*           local_free;        // list of deferred free blocks by this thread (migrates to `free`)

  #if (MI_ENCODE_FREELIST || MI_PADDING)
  uintptr_t             keys[2];           // two random keys to encode the free lists (see `_mi_block_next`) or padding canary
  #endif

  mi_thread_free_t xthread_free;  // list of deferred free blocks freed by other threads
  uintptr_t        xheap;

  struct mi_page_s*     next;              // next page owned by this thread with the same `block_size`
  struct mi_page_s*     prev;              // previous page owned by this thread with the same `block_size`
    mi_malloc_kind_t    malloc_kind;
    pthread_mutex_t mutex;
    void* page_start;
} mi_page_t;

typedef struct mi_segment_migrate {
  // constant fields
  bool                 allow_decommit;
  bool                 allow_purge;
  uintptr_t             segment_start;
  size_t               segment_size;     // for huge pages this may be different from `MI_SEGMENT_SIZE`
  
  // segment fields
  struct mi_segment_s* abandoned_next;
  struct mi_segment_s* next;             // must be the first segment field after abandoned_next -- see `segment.c:segment_init`
  struct mi_segment_s* prev;

  size_t               abandoned;        // abandoned pages (i.e. the original owning thread stopped) (`abandoned <= used`)
  size_t               abandoned_visits; // count how often this segment is visited in the abandoned list (to force reclaim if it is too long)

  size_t               used;             // count of pages in use (`used <= capacity`)
  size_t               capacity;         // count of available pages (`#free + used`)
  size_t               segment_info_size;// space we are using from the first page for segment meta-data and possible guard pages.
  uintptr_t            cookie;           // verify addresses in secure mode: `_mi_ptr_cookie(segment) == segment->cookie`

  // layout like this to optimize access in `mi_free`
  size_t                 page_shift;     // `1 << page_shift` == the page sizes == `page->block_size * page->reserved` (unless the first page, then `-segment_info_size`).
  mi_threadid_t thread_id;      // unique id of the thread owning this segment
  mi_page_kind_t       page_kind;        // kind of pages: small, medium, large, or huge
  mi_malloc_kind_t      malloc_kind;
  mi_page_t            pages[64];         // up to `MI_SMALL_PAGES_PER_SEGMENT` pages
};


}  // namespace kv
