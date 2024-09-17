include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(microbench_common microbench_common.cc)

target_link_libraries(microbench_common
    mralloc
    ibverbs
    rdmacm
    rt
    profiler
)
