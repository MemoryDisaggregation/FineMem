/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-08-12 22:24:28
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-11-14 17:48:17
 * @FilePath: /rmalloc_newbase/source/client.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/floatn.h>
#include <bits/stdint-uintn.h>
#include <bits/types/FILE.h>
#include <linux/mman.h>
#include <pthread.h>
#include <atomic>
#include <cassert>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>
#include <iostream>
#include "computing_node.h"
#include <sys/mman.h>
#include <sys/time.h>

const int thread_num = 12;

pthread_barrier_t start_barrier;
pthread_barrier_t end_barrier;
std::ofstream result;
pthread_mutex_t file_lock;

std::atomic<int> record_global[10];
std::atomic<uint64_t> avg;

void* fetch_mem(void* arg) {
    uint64_t avg_time_ = 0;
    uint64_t count_ = 0;
    uint64_t cdf_counter[10];
    uint64_t max_time_ = 0;
    struct timeval start, end;
    mralloc::ComputingNode* heap = (mralloc::ComputingNode*)arg;
    int record[10] = {0};
    uint64_t addr[32]; uint32_t rkey[32];
    
    for(int j = 0; j < 4; j ++) {
        pthread_barrier_wait(&start_barrier);
        gettimeofday(&start, NULL);
        for(int i = 0; i < 32; i ++){
            // addr[i] = (uint64_t)mmap(NULL, 1024*1024*4, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
            heap->fetch_mem_block_nocached(addr[i], rkey[i]);
            // heap->fetch_mem_one_sided(addr[i], rkey[i]);
            // if(addr[i] != -1)
            //     printf("%lx,  %u\n", addr[i], rkey[i]);
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        char buffer[2][16] = {"aaa", "bbb"};
        char read_buffer[4];
        // for(int i = 0; i < 32; i ++){
        //     // heap->fetch_mem_fast_remote(addr, rkey);
        //     heap->get_conn()->remote_write(buffer[i%2], 64, addr[i], rkey[i]);
        //     heap->get_conn()->remote_read(read_buffer, 4, addr[i], rkey[i]);
        //     // printf("%lx,  %u\n", addr[i], rkey[i]);
        //     assert(read_buffer[0] == buffer[i%2][0]);
        // }        
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
        time = time / 32;
        avg_time_ = (avg_time_*count_ + time)/(count_ + 1);
        count_ += 1;
        sleep(1);
    }
    printf("avg time:%lu, max_time:%lu\n", avg_time_, max_time_);
    for(int i=0;i<10;i++){
        record_global[i].fetch_add(record[i]);
    }
    avg.fetch_add(avg_time_);
    return NULL;
}

int main(int argc, char* argv[]){

    if(argc < 3){
        printf("Usage: %s <ip> <port>\n", argv[0]);
        return 0;
    }

    std::string ip = argv[1];
    std::string port = argv[2];

    bool multitest = false;
    if(!multitest) {
        mralloc::ComputingNode* heap = new mralloc::ComputingNode(true, true, true);
        heap->start(ip, port);

        // << single thread, local test, fetch remote memory >>
        int iter = 5;
        uint64_t addr;
        uint32_t rkey=0;
        char buffer[2][64*1024] = {"aaa", "bbb"};
        char read_buffer[4];
        while(iter--){
            heap->fetch_mem_block(addr, rkey);
            heap->show_ring_length();
            std::cout << "write addr: " << std::hex << addr << " rkey: " << std::dec <<rkey << std::endl;
            for(int i = 0; i < 2; i++)
                heap->get_conn()->remote_write(buffer[iter%2], 64, addr+i*64, rkey);
            std::cout << "read addr: " << std::hex << addr << " rkey: " << std::dec <<rkey << std::endl;
            for(int i = 0; i < 2; i++)
                heap->get_conn()->remote_read(read_buffer, 4, addr, rkey);
            printf("alloc: %lx : %u, content: %s\n", addr, rkey, read_buffer);
            heap->free_mem_block(addr);
            heap->show_ring_length();
        // heap->mr_bind_remote(2*1024*1024, addr, rkey, 114514);
        // std::cout << "addr mw bind success " << std::endl;
        }
        getchar();
        heap->stop();
        delete heap;
    }

    // << multiple thread, local test, fetch remote memory >>
    else {
        mralloc::ComputingNode* client[thread_num];
        result.open("result.csv");
        for(int i=0;i<10;i++) {
            record_global[i].store(0);
        }
        avg.store(0);
        pthread_mutex_init(&file_lock, NULL);
        pthread_barrier_init(&start_barrier, NULL, thread_num);
        pthread_barrier_init(&end_barrier, NULL, thread_num);
        pthread_t running_thread[thread_num];
        for(int i = 0; i < thread_num; i++) {
            client[i] = new mralloc::ComputingNode(false, false, true);
            client[i]->start(ip, port);
            printf("thread %d\n init success\n", i);
            pthread_create(&running_thread[i], NULL, fetch_mem, client[i]);
        }
        for(int i = 0; i < thread_num; i++) {
            pthread_join(running_thread[i], NULL);
        }
        for(int i = 0; i < thread_num; i++) {
            client[i]->stop();
        }
        for(int i=0;i<10;i++)
            result << record_global[i].load() << std::endl;
        result.close();
        printf("total avg: %luus\n", avg.load()/thread_num);
    }

    return 0;

}