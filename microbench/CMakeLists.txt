include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(microbench_common microbench_common.cc)

target_link_libraries(microbench_common
    mralloc
    ibverbs
    rdmacm
    rt
    profiler
)

add_executable(v_microbench v_microbench.cc)

target_link_libraries(v_microbench
    mralloc
    ibverbs
    rdmacm
    rt
    profiler
    memkind
)