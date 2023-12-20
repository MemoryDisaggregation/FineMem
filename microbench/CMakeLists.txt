include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(protocol_exclusive_test protocol_exclusive_test.cc)

target_link_libraries(protocol_exclusive_test
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(protocol_share_test protocol_share_test.cc)

target_link_libraries(protocol_share_test
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(protocol_class_test protocol_class_test.cc)

target_link_libraries(protocol_class_test
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(protocol_mmap_test protocol_mmap_test.cc)

target_link_libraries(protocol_mmap_test
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(protocol_rpc_test protocol_rpc_test.cc)

target_link_libraries(protocol_rpc_test
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(protocol_cxl_shm_test protocol_cxl_shm_test.cc)

target_link_libraries(protocol_cxl_shm_test
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(microbench_common microbench_common.cc)

target_link_libraries(microbench_common
    mralloc
    ibverbs
    rdmacm
    rt
)

add_executable(microbench_class microbench_class.cc)

target_link_libraries(microbench_class
    mralloc
    ibverbs
    rdmacm
    rt
)