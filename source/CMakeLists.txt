include_directories(${PROJECT_SOURCE_DIR}/include)

set(BASE_SOURCE
    local_engine.cc remote_engine.cc rdma_conn_manager.cc rdma_conn.cc rdma_mem_pool.cc free_block_manager.cc local_heap.cc remote_heap.cc
    )

add_library(mralloc STATIC ${BASE_SOURCE})

add_executable(server server.cc)

add_executable(client client.cc)

add_executable(user_code user_code.cc)
# add_executable(remote_heap remote_heap.cc)

# message("Build Type: ${CMAKE_BUILD_TYPE} ${CMAKE_CXX_FLAGS}")

target_link_libraries(server
    profiler
    mralloc
    ibverbs
    rdmacm
    rt
)

target_link_libraries(client
    mralloc
    ibverbs
    rdmacm
    rt
)


target_link_libraries(user_code
    mralloc
    ibverbs
    rdmacm
    rt    
)

# target_link_libraries(remote_heap
#     polarkv
#     ibverbs
#     rdmacm
# )

install(TARGETS mralloc
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
