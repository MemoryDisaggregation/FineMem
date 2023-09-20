set(BASE_INCLUDE
    kv_engine.h msg.h rdma_conn.h rdma_conn_manager.h rdma_mem_pool.h local_heap.h remote_heap.h free_block_manager.h memory_heap.h rpc_server.h)

install(FILES   ${BASE_INCLUDE}
    DESTINATION include)