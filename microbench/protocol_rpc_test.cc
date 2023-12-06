
#include <pthread.h>
#include <sys/select.h>
#include <cstdio>
#include <fstream>
#include "free_block_manager.h"
#include "msg.h"
#include "rdma_conn.h"
#include "rdma_conn_manager.h"
#include <sys/time.h>

const int iteration = 32;
const int epoch = 4;

pthread_barrier_t start_barrier;
pthread_barrier_t end_barrier;
std::ofstream malloc_result;
std::ofstream free_result;
pthread_mutex_t file_lock;

std::atomic<int> malloc_record_global[10];
std::atomic<int> free_record_global[10];
std::atomic<uint64_t> malloc_avg;
std::atomic<uint64_t> free_avg;
std::atomic<uint64_t> core_id;

void* worker(void* arg) {
    uint64_t malloc_avg_time_ = 0, free_avg_time_ = 0;
    uint64_t malloc_count_ = 0, free_count_ = 0;
    struct timeval start, end;
    mralloc::ConnectionManager* conn = (mralloc::ConnectionManager*)arg;
    int malloc_record[10] = {0};
    int free_record[10] = {0};
    uint64_t addr[iteration]; uint32_t rkey[iteration];
    
    for(int j = 0; j < epoch; j ++) {
        // malloc
        pthread_barrier_wait(&start_barrier);
        gettimeofday(&start, NULL);
        for(int i = 0; i < iteration; i ++){
            conn->remote_fetch_block(addr[i], rkey[i]);
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        
        // valid check
        char buffer[2][16] = {"aaa", "bbb"};
        char read_buffer[4];
        for(int i = 0; i < iteration; i ++){
            // printf("try to access %p:%u\n", addr[i], rkey[i]);
            conn->remote_write(buffer[i%2], 64, addr[i], rkey[i]);
            conn->remote_read(read_buffer, 4, addr[i], rkey[i]);
            assert(read_buffer[0] == buffer[i%2][0]);
        }        
        uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        uint64_t log10 = 0;
        uint64_t log_time = time;
        while(log_time/10 > 0){
            log_time = log_time / 10;
            log10 += 1;           
        }
        malloc_record[log10] += 1;
        time = time / iteration;
        malloc_avg_time_ = (malloc_avg_time_*malloc_count_ + time)/(malloc_count_ + 1);
        malloc_count_ += 1;
        
        // free
        pthread_barrier_wait(&start_barrier);
        gettimeofday(&start, NULL);
        for(int i = 0; i < iteration; i ++){
            // printf("try to free %p\n", addr[i]);
            conn->remote_free_block(addr[i]);
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        log10 = 0; log_time = time;
        while(log_time/10 > 0){
            log_time = log_time / 10;
            log10 += 1;           
        }
        free_record[log10] += 1;
        time = time / iteration;
        free_avg_time_ = (free_avg_time_*free_count_ + time)/(free_count_ + 1);
        free_count_ += 1;
    }
    // printf("avg time:%lu, max_time:%lu\n", avg_time_, max_time_);
    for(int i=0;i<10;i++){
        malloc_record_global[i].fetch_add(malloc_record[i]);
        free_record_global[i].fetch_add(free_record[i]);
    }
    malloc_avg.fetch_add(malloc_avg_time_);
    free_avg.fetch_add(free_avg_time_);
    return NULL;
}

int main(int argc, char* argv[]) {
    if(argc < 4){
        printf("Usage: %s <ip> <port> <thread>\n", argv[0]);
        return 0;
    }

    std::string ip = argv[1];
    std::string port = argv[2];
    int thread_num = atoi(argv[3]);
    std::ofstream result;
    result.open("result.csv");
    
    for(int i=0;i<10;i++){
        malloc_record_global[i].store(0);
        free_record_global[i].store(0);
    }
    malloc_avg.store(0);
    free_avg.store(0);
    pthread_mutex_init(&file_lock, NULL);
    pthread_barrier_init(&start_barrier, NULL, thread_num);
    pthread_barrier_init(&end_barrier, NULL, thread_num);
    pthread_t running_thread[thread_num];
    mralloc::ConnectionManager* conn[thread_num];
    for(int i = 0; i < thread_num; i++) {
        conn[i] = new mralloc::ConnectionManager();
        conn[i]->init(ip, port, 1, 1);
        pthread_create(&running_thread[i], NULL, worker, conn[i]);
    }
    for(int i = 0; i < thread_num; i++) {
        pthread_join(running_thread[i], NULL);
    }
    for(int i=0;i<10;i++) {
        result << "malloc " << i << " " <<malloc_record_global[i].load() << std::endl;
        result << "free " << i << " " <<free_record_global[i].load() << std::endl;
    }
    result.close();
    printf("total malloc avg: %luus\n", malloc_avg.load()/thread_num);
    printf("total free avg: %luus\n", free_avg.load()/thread_num);
}