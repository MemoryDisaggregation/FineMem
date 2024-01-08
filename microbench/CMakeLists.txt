include_directories(${PROJECT_SOURCE_DIR}/include)

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

add_executable(attacker attacker.cc)

target_link_libraries(attacker
    mralloc
    ibverbs
    rdmacm
    rt
)