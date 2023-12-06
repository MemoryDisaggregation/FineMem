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
