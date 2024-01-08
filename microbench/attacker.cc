
#include <pthread.h>
#include <sys/select.h>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <unordered_map>
#include "free_block_manager.h"
#include "msg.h"
#include "rdma_conn.h"
#include "rdma_conn_manager.h"
#include "cpu_cache.h"
#include <sys/time.h>

const int iteration = 128;
const int epoch = 256;

enum alloc_type { cxl_shm_alloc, fusee_alloc, rpc_alloc, share_alloc, exclusive_alloc, pool_alloc };

enum test_type { stage_test, shuffle_test, short_test };

// alloc_type type = cxl_shm_alloc;
alloc_type type = share_alloc;

test_type test = short_test;

pthread_barrier_t start_barrier;
pthread_barrier_t end_barrier;
std::ofstream malloc_result;
std::ofstream free_result;
pthread_mutex_t file_lock;

std::atomic<int> malloc_record_global[1000];
std::atomic<int> free_record_global[1000];
double malloc_avg[128];
std::atomic<uint64_t> free_avg;
std::atomic<uint64_t> id;

void* worker(void* arg) {
    uint64_t thread_id = id.fetch_add(1);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int id_ = id+51;
    CPU_SET(id_, &cpuset);
    pthread_t this_tid = pthread_self();
    uint64_t ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    // assert(ret == 0);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("client %d main process running on core: %d\n",id_ , i);
        }
    }
    mralloc::ConnectionManager* conn = (mralloc::ConnectionManager*)arg;

    if(thread_id == 1) {
    	getchar();
    }
    mralloc::cpu_cache* cpu_cache_;
    cpu_cache_ = new mralloc::cpu_cache(4*1024*1024);
    uint64_t malloc_avg_time_ = 0, free_avg_time_ = 0;
    uint64_t malloc_count_ = 0, free_count_ = 0;
    struct timeval start, end;
    int malloc_record[1000] = {0};
    int free_record[1000] = {0};
    uint64_t addr; uint32_t rkey;
    uint64_t current_index = 0;
    int rand_iter = iteration;
    for(int j = 0; j < epoch; j ++) {
        pthread_barrier_wait(&start_barrier);
        gettimeofday(&start, NULL);
        for(int i = 0; i < 144; i ++){
            if(!cpu_cache_->fetch_cache(i, addr, rkey) || addr == 0 || addr == -1){
                printf("alloc false\n");
            }
            if(addr != -1)
                cpu_cache_->add_free_cache(i, addr);
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        printf("epoch %d malloc finish\n", j);
        
        if (thread_id == 1)
            conn->remote_print_alloc_info();
            
        uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        time = time / rand_iter;
        if(time < 1000)
            malloc_record[(int)time] += 1;
        malloc_avg_time_ = (malloc_avg_time_*malloc_count_ + time)/(malloc_count_ + 1);
        malloc_count_ += 1;
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
    }
    for(int i=0;i<1000;i++){
        malloc_record_global[i].fetch_add(malloc_record[i]);
    }
    malloc_avg[thread_id] = malloc_avg_time_;
    return NULL;
}

int main(int argc, char* argv[]) {
    if(argc < 4){
        printf("Usage: %s <ip> <port> <thread> \n", argv[0]);
        return 0;
    }

    std::string ip = argv[1];
    std::string port = argv[2];
    int thread_num = atoi(argv[3]);

    std::ofstream result;
    result.open("attacker_" + std::string(argv[3]) +  "_.csv");
    
    for(int i=0;i<1000;i++){
        malloc_record_global[i].store(0);
        free_record_global[i].store(0);
    }
    id.store(0);
    for(int i = 0; i < 128; i++)
        malloc_avg[i] = 0;
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
    result << "malloc " << std::endl;
    for(int i=0;i<1000;i++) {
        if(malloc_record_global[i].load() != 0)
            result << i << " " <<malloc_record_global[i].load() << std::endl;
    }
    result << "free " << std::endl;
    for(int i=0;i<1000;i++) {
        if(free_record_global[i].load() != 0)
            result << i << " " <<free_record_global[i].load() << std::endl;
    }
    double malloc_avg_final = 0;
    for(int i = 0; i < 128; i++) {
        malloc_avg_final += malloc_avg[i];
    }
    printf("total malloc avg: %lfus\n", malloc_avg_final/thread_num);
    result << "total malloc avg: " << malloc_avg_final/thread_num << std::endl;
    printf("total free avg: %luus\n", free_avg.load()/thread_num);
    result << "total free avg: " << free_avg.load()/thread_num << std::endl;
    result.close();
}
