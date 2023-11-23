/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-09-13 22:28:40
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-14 16:33:22
 * @FilePath: /rmalloc_newbase/source/user_code.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/floatn.h>
#include <bits/stdint-uintn.h>
#include <bits/types/FILE.h>
#include <pthread.h>
#include <atomic>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <iostream>
#include "memory_heap.h"
#include <sys/time.h>

const uint64_t cache_size = 1024*4*1024;

const uint64_t iter_num = 128;

const uint64_t epoch_num = 4;

const int thread_num = 1;

pthread_barrier_t start_barrier;
pthread_barrier_t end_barrier;
std::ofstream result;
pthread_mutex_t file_lock;

std::atomic<int> record_global[10];
std::atomic<uint64_t> avg;
std::atomic<uint64_t> core_id;

mralloc::ConnectionManager* m_rdma_conn_;

void* fetch_mem(void* arg) {

    // cpu_set_t cpuset;
    // CPU_ZERO(&cpuset);
    // int id = core_id.fetch_add(1);
    // CPU_SET(id, &cpuset);
    // pthread_t this_tid = pthread_self();
    // uint64_t ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    // // assert(ret == 0);
    // ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    // for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
    //     if (CPU_ISSET(i, &cpuset)) {
    //         printf("client %d main process running on core: %d\n",id , i);
    //     }
    // }

    uint64_t avg_time_ = 0;
    uint64_t count_ = 0;
    uint64_t cdf_counter[10];
    uint64_t max_time_ = 0;
    struct timeval start, end;
    mralloc::cpu_cache cpu_cache_ = mralloc::cpu_cache(cache_size);
    int record[10] = {0};
    uint64_t addr[iter_num]; uint32_t rkey[iter_num];
    
    for(int j = 0; j < epoch_num; j ++) {
        pthread_barrier_wait(&start_barrier);
        gettimeofday(&start, NULL);
        for(int i = 0; i < iter_num; i ++){
            bool result;
            unsigned cpu;
            unsigned node;
            if(sched_getcpu(&cpu,&node)==-1){
                printf("sched_getcpu bad \n");
                return NULL;
            }
            result = cpu_cache_.fetch_cache(cpu, addr[i], rkey[i]); 
            if (result == false) {
                printf("impossible!\n");
            } 
                // printf("%lx,  %u\n", addr[i], rkey[i]);
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        char buffer[2][16] = {"aaa", "bbb"};
        char read_buffer[4];
        for(int i = 0; i < 16; i ++){
            // heap->fetch_mem_fast_remote(addr, rkey);
            m_rdma_conn_->remote_write(buffer[i%2], 64, addr[i], rkey[i]);
            m_rdma_conn_->remote_read(read_buffer, 4, addr[i], rkey[i]);
            // printf("%lx,  %u\n", addr[i], rkey[i]);
            assert(read_buffer[0] == buffer[i%2][0]);
        }        
        uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        // uint64_t log10 = 0;
        // while(time/10 > 0){
        //     time = time / 10;
        //     log10 += 1;           
        // }
        // record[log10] += 1;
        // std::stringstream buffer;
        // buffer << time << std::endl;
        if(time > max_time_) max_time_ = time;
        time = time / iter_num;
        avg_time_ = (avg_time_*count_ + time)/(count_ + 1);
        count_ += 1;
    }
    printf("avg time:%lu, max_time:%lu\n", avg_time_, max_time_);
    for(int i=0;i<10;i++){
        record_global[i].fetch_add(record[i]);
    }
    avg.fetch_add(avg_time_);
    return NULL;
}

int main(int argc, char** argv){
    // mralloc::cpu_cache cpu_cache_ = mralloc::cpu_cache(cache_size);
    m_rdma_conn_ = new mralloc::ConnectionManager();
    // if (m_rdma_conn_ == nullptr) return -1;
    if (m_rdma_conn_ == nullptr || m_rdma_conn_->init("10.0.0.63", "1145", 2, 20) == -1 ){
        printf("rdma connection create failed!\n");
    }

    // << multi thread test >>
    result.open("result.csv");
    for(int i=0;i<10;i++)
        record_global[i].store(0);
    avg.store(0);
    core_id.store(0);
    pthread_mutex_init(&file_lock, NULL);
    pthread_barrier_init(&start_barrier, NULL, thread_num);
    pthread_barrier_init(&end_barrier, NULL, thread_num);
    pthread_t running_thread[thread_num];
    for(int i = 0; i < thread_num; i++) {
        pthread_create(&running_thread[i], NULL, fetch_mem, NULL);
    }
    for(int i = 0; i < thread_num; i++) {
        pthread_join(running_thread[i], NULL);
    }
    for(int i=0;i<10;i++)
        result << record_global[i].load() << std::endl;
    result.close();
    printf("total avg: %luus\n", avg.load()/thread_num);


    // << single thread test >>
    // int k=1;
    // uint64_t addr; uint32_t rkey;
    // char buffer1[5] = "aaaa";
    // char buffer2[5] = "bbbb";
    // char read_buffer[5];
    // while(k--){
    //     cpu_cache_.fetch_cache(1, addr, rkey);
    //     m_rdma_conn_->remote_write(buffer1, 5, addr, rkey);
    //     m_rdma_conn_->remote_read(read_buffer, 5, addr, rkey);
    //     printf("alloc: %lx : %u, content: %s\n", addr, rkey, read_buffer);
    //     // rkey should be used by server side, client side has no necessary to support rkey
    //     uint32_t newkey;
    //     m_rdma_conn_->remote_mw(addr, rkey, cache_size, newkey);
    //     m_rdma_conn_->remote_write(buffer2, 5, addr + cache_size/2, newkey);
    //     m_rdma_conn_->remote_read(read_buffer, 5, addr + cache_size/2, newkey);
    //     printf("using new key: %lx : %u, content: %s\n", addr, newkey, read_buffer);
    //     // m_rdma_conn_->remote_write(buffer1, 5, addr, rkey);
    //     // m_rdma_conn_->remote_read(read_buffer, 5, addr, rkey);
    //     // printf("using old key: %lx : %u, content: %s\n", addr, rkey, read_buffer);
    // }
    return 0;
}