
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
const int epoch = 64;

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
std::atomic<uint64_t> malloc_avg;
std::atomic<uint64_t> free_avg;
std::atomic<uint64_t> id;

class test_allocator{
public:
    test_allocator() {};
    virtual bool malloc(uint64_t &addr, uint32_t &rkey) {};
    virtual bool free(uint64_t addr) {};
    ~test_allocator() {};
};

class cxl_shm_allocator : test_allocator{
public:
    cxl_shm_allocator(mralloc::ConnectionManager* conn, uint64_t start_hint) {
        conn_ = conn;
        current_index_ = start_hint;
    }
    ~cxl_shm_allocator() {};
    bool malloc(uint64_t &addr, uint32_t &rkey) override {
        return conn_->fetch_block(current_index_, addr, rkey);
    };
    bool free(uint64_t addr) override {
        return conn_->free_block(addr);
    };
private:
    uint64_t current_index_;
    mralloc::ConnectionManager* conn_;
};

class fusee_allocator : test_allocator{
public:
    fusee_allocator(mralloc::ConnectionManager* conn) {
        conn_ = conn;
    }
    ~fusee_allocator() {};
    bool malloc(uint64_t &addr, uint32_t &rkey) override {
        return !conn_->remote_fetch_block(addr, rkey);
    };
    bool free(uint64_t addr) override {
        return !conn_->remote_free_block(addr);
    };
private:
    mralloc::ConnectionManager* conn_;
};

class rpc_allocator : test_allocator{
public:
    rpc_allocator(mralloc::ConnectionManager* conn) {
        conn_ = conn;
    }
    ~rpc_allocator() {};
    bool malloc(uint64_t &addr, uint32_t &rkey) override {
        return !conn_->register_remote_memory(addr, rkey, 4*1024*1024);
    };
    bool free(uint64_t addr) override {
        return !conn_->unregister_remote_memory(addr);
    };
private:
    mralloc::ConnectionManager* conn_;
};

class share_allocator : test_allocator{
public:
    share_allocator(mralloc::ConnectionManager* conn) {
        conn_ = conn;
        conn_->find_section(0, cache_section, cache_section_index, mralloc::alloc_no_class);
        conn_->fetch_region(cache_section, cache_section_index, 0, true, cache_region);
    }
    ~share_allocator() {};
    bool malloc(uint64_t &addr, uint32_t &rkey) override {
        while(!conn_->fetch_region_block(cache_region, addr, rkey, false)){
            while(!conn_->fetch_region(cache_section, cache_section_index, 0, true, cache_region)){
                if(!conn_->find_section(0, cache_section, cache_section_index, mralloc::alloc_no_class)){
                    return false;
                }
            }
        }
        return true;
    };
    bool free(uint64_t addr) override {
        int result = conn_->free_region_block(addr, false);
        if(result == -2 && conn_->get_addr_region_index(addr) != cache_region.offset_) {
            conn_->set_region_empty(cache_region);
        }
        return true;
    };
private:
    uint32_t cache_section_index;
    mralloc::section_e cache_section;
    mralloc::region_e cache_region;
    mralloc::ConnectionManager* conn_;
};

class exclusive_allocator : test_allocator{
public:
    exclusive_allocator(mralloc::ConnectionManager* conn) {
        conn_ = conn;
        conn->find_section(0, cache_section, cache_section_index, mralloc::alloc_empty);
        conn->fetch_region(cache_section, cache_section_index, 0, false, cache_region.region);
        conn->fetch_exclusive_region_rkey(cache_region.region, cache_region.rkey);
        region_record[cache_region.region.offset_] = cache_region;
    }
    ~exclusive_allocator() {};
    bool malloc(uint64_t &addr, uint32_t &rkey) override {
        int index = 0 ;
        while((index = mralloc::find_free_index_from_bitmap32_tail(cache_region.region.base_map_)) == -1 ){
            bool cache_useful = false;
            region_record[cache_region.region.offset_].region = cache_region.region;
            for(auto iter = region_record.begin(); iter != region_record.end(); iter ++) {
                if((index = mralloc::find_free_index_from_bitmap32_tail(iter->second.region.base_map_)) != -1){
                    cache_region = iter->second;
                    cache_useful = true;
                    break;
                }
            }
            if(!cache_useful) {
                while(!conn_->fetch_region(cache_section, cache_section_index, 0, false, cache_region.region)){
                    if(!conn_->find_section(0, cache_section, cache_section_index, mralloc::alloc_empty)) {
                        return false;
                    }
                }
                conn_->fetch_exclusive_region_rkey(cache_region.region, cache_region.rkey);
                region_record[cache_region.region.offset_] = cache_region;
            }
        }
        cache_region.region.base_map_ |= 1<<index;
        addr = conn_->get_region_block_addr(cache_region.region, index);
        rkey = cache_region.rkey[index];
        return true;
    };
    bool free(uint64_t addr) override {
        uint16_t index = conn_->get_addr_region_index(addr);
        uint32_t offset = conn_->get_addr_region_offset(addr);
        if(index == cache_region.region.offset_) {
            cache_region.region.base_map_ &= ~(uint32_t)(1<<offset);
            // conn_->remote_rebind(addr, 0, cache_region.rkey[offset]);
        } else {
            region_record[index].region.base_map_ &= ~(uint32_t)(1<<offset);
            // conn_->remote_rebind(addr, 0, region_record[index].rkey[offset]);
        }
        return true;
    };
private:
    uint32_t cache_section_index;
    mralloc::section_e cache_section;
    mralloc::region_with_rkey cache_region;
    std::unordered_map<uint16_t, mralloc::region_with_rkey> region_record;
    mralloc::ConnectionManager* conn_;
};

class pool_allocator : test_allocator{
public:
    pool_allocator() {
        cpu_cache_ = new mralloc::cpu_cache(4*1024*1024);
    }
    ~pool_allocator() {};
    bool malloc(uint64_t &addr, uint32_t &rkey) override {
        return cpu_cache_->fetch_cache(addr, rkey);
    };
    bool free(uint64_t addr) override {
        cpu_cache_->add_free_cache(addr);
        return true;
    };
private:
    mralloc::cpu_cache* cpu_cache_;
};

void warmup(test_allocator* alloc) {
    uint64_t addr;
    uint32_t rkey;
    for(int i = 0; i < iteration; i++) {
        if(!alloc->malloc(addr, rkey)){
            printf("warmup malloc failed\n");
        }
        if(!alloc->free(addr)){
            printf("warmup free failed\n");
        }
    }
}

void stage_alloc(mralloc::ConnectionManager* conn, test_allocator* alloc, uint64_t thread_id) {
    uint64_t malloc_avg_time_ = 0, free_avg_time_ = 0;
    uint64_t malloc_count_ = 0, free_count_ = 0;
    struct timeval start, end;
    int malloc_record[1000] = {0};
    int free_record[1000] = {0};
    uint64_t addr[iteration]; uint32_t rkey[iteration];
    uint64_t current_index = 0;
    int rand_iter = iteration;
    for(int j = 0; j < epoch; j ++) {
        pthread_barrier_wait(&start_barrier);
	    gettimeofday(&start, NULL);
        for(int i = 0; i < rand_iter; i ++){
            if(addr[i] != 0 && rkey[i] != 0) 
                continue;
            if(!alloc->malloc(addr[i], rkey[i])){
                printf("alloc false\n");
            }
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        printf("epoch %d malloc finish\n", j);
        if (thread_id == 1)
            conn->remote_print_alloc_info();

        // valid check
        char buffer[2][16] = {"aaa", "bbb"};
        char read_buffer[4];
        for(int i = 0; i < rand_iter; i ++){
            if(conn->remote_write(buffer[i%2], 64, addr[i], rkey[i])) {
                printf("wrong write addr %p, %u\n", addr[i], rkey[i]);
            }
            if(conn->remote_read(read_buffer, 4, addr[i], rkey[i])) {
                printf("wrong read addr %p, %u\n", addr[i], rkey[i]);
            }
            // printf("access addr %p, %u\n", addr[i], rkey[i]);
            // assert(read_buffer[0] == buffer[i%2][0]);
        }        
        uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        time = time / rand_iter;
        if(time < 1000)
            malloc_record[time] += 1;
        malloc_avg_time_ = (malloc_avg_time_*malloc_count_ + time)/(malloc_count_ + 1);
        malloc_count_ += 1;
        printf("epoch %d check finish\n", j);
        
        // free
        pthread_barrier_wait(&start_barrier);
        gettimeofday(&start, NULL);
        int result;
        for(int i = 0; i < rand_iter; i ++){
            if(rand()%100 > 20 && addr[i] != 0){
                if(!alloc->free(addr[i]))
                    printf("free error!\n");
                addr[i] = 0;
                rkey[i] = 0;
            }
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        time = time / rand_iter;
        if(time < 1000)
            free_record[time] += 1;
        free_avg_time_ = (free_avg_time_*free_count_ + time)/(free_count_ + 1);
        free_count_ += 1;
        printf("epoch %d free finish\n", j);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (thread_id == 1)
            conn->remote_print_alloc_info();
    }
    for(int i=0;i<1000;i++){
        malloc_record_global[i].fetch_add(malloc_record[i]);
        free_record_global[i].fetch_add(free_record[i]);
    }
    malloc_avg.fetch_add(malloc_avg_time_);
    free_avg.fetch_add(free_avg_time_);
}

void shuffle_alloc(mralloc::ConnectionManager* conn, test_allocator* alloc, uint64_t thread_id) {
    uint64_t malloc_avg_time_ = 0, free_avg_time_ = 0;
    uint64_t malloc_count_ = 0, free_count_ = 0;
    struct timeval start, end;
    int malloc_record[1000] = {0};
    int free_record[1000] = {0};
    uint64_t addr[iteration]; uint32_t rkey[iteration];
    uint64_t current_index = 0;
    int rand_iter = iteration;
    if(thread_id % 2 == 0){
        pthread_barrier_wait(&start_barrier);
        for(int j = 0; j < epoch; j ++) {
            // pthread_barrier_wait(&start_barrier);
            gettimeofday(&start, NULL);
            for(int i = 0; i < rand_iter; i ++){
                if(addr[i] != 0 && rkey[i] != 0) 
                    continue;
                if(!alloc->malloc(addr[i], rkey[i]) || addr[i] == 0){
                    printf("alloc false\n");
                }
            }
            gettimeofday(&end, NULL);
            // pthread_barrier_wait(&end_barrier);
            printf("epoch %d malloc finish\n", j);
            if (thread_id == 1)
                conn->remote_print_alloc_info();
                
            // valid check
            // char buffer[2][16] = {"aaa", "bbb"};
            // char read_buffer[4];
            // for(int i = 0; i < rand_iter; i ++){
            //     conn->remote_write(buffer[i%2], 64, addr[i], rkey[i]);
            //     conn->remote_read(read_buffer, 4, addr[i], rkey[i]);
            //     // printf("access addr %p\n", addr[i]);
            //     assert(read_buffer[0] == buffer[i%2][0]);
            // }        
            uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
            time = time / rand_iter;
            if(time < 1000)
                malloc_record[time] += 1;
            malloc_avg_time_ = (malloc_avg_time_*malloc_count_ + time)/(malloc_count_ + 1);
            malloc_count_ += 1;
            printf("epoch %d check finish\n", j);
            
            // free
            // pthread_barrier_wait(&start_barrier);
            gettimeofday(&start, NULL);
            int result;
            for(int i = 0; i < rand_iter; i ++){
                if(rand()%100 > 20 && addr[i] != 0){
                    if(!alloc->free(addr[i]))
                        printf("free error!\n");
                    addr[i] = 0;
                    rkey[i] = 0;
                }
            }
            gettimeofday(&end, NULL);
            // pthread_barrier_wait(&end_barrier);
            time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
            time = time / rand_iter;
            if(time < 1000)
                free_record[time] += 1;
            free_avg_time_ = (free_avg_time_*free_count_ + time)/(free_count_ + 1);
            free_count_ += 1;
            printf("epoch %d free finish\n", j);
            // std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (thread_id == 1)
                conn->remote_print_alloc_info();
        }
    } else {
        for(int i = 0; i < rand_iter; i ++){
            if(addr[i] != 0 && rkey[i] != 0) 
                continue;
            if(!alloc->malloc(addr[i], rkey[i])|| addr[i] == 0){
                printf("alloc false\n");
            }
        }
        pthread_barrier_wait(&start_barrier);
        for(int j = 0; j < epoch; j ++) {
            // valid check
            // char buffer[2][16] = {"aaa", "bbb"};
            // char read_buffer[4];
            // for(int i = 0; i < rand_iter; i ++){
            //     conn->remote_write(buffer[i%2], 64, addr[i], rkey[i]);
            //     conn->remote_read(read_buffer, 4, addr[i], rkey[i]);
            //     // printf("access addr %p\n", addr[i]);
            //     assert(read_buffer[0] == buffer[i%2][0]);
            // }        
            
            // free
            // pthread_barrier_wait(&start_barrier);
            gettimeofday(&start, NULL);
            for(int i = 0; i < rand_iter; i ++){
                if(rand()%100 > 20 && addr[i] != 0){
                    if(!alloc->free(addr[i]))
                        printf("free error!\n");
                    addr[i] = 0;
                    rkey[i] = 0;
                }
            }
            gettimeofday(&end, NULL);
            // pthread_barrier_wait(&end_barrier);
            uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
            time = time / rand_iter;
            if(time < 1000)
                free_record[time] += 1;
            free_avg_time_ = (free_avg_time_*free_count_ + time)/(free_count_ + 1);
            free_count_ += 1;
            printf("epoch %d free finish\n", j);
            // std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (thread_id == 1)
                conn->remote_print_alloc_info();
            
            // pthread_barrier_wait(&start_barrier);
            gettimeofday(&start, NULL);
            for(int i = 0; i < rand_iter; i ++){
                if(addr[i] != 0 && rkey[i] != 0) 
                    continue;
                if(!alloc->malloc(addr[i], rkey[i])|| addr[i] == 0){
                    printf("alloc false\n");
                }
            }
            gettimeofday(&end, NULL);
            // pthread_barrier_wait(&end_barrier);
            printf("epoch %d malloc finish\n", j);
            if (thread_id == 1)
                conn->remote_print_alloc_info();
                
            time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
            time = time / rand_iter;
            if(time < 1000)
                malloc_record[time] += 1;
            malloc_avg_time_ = (malloc_avg_time_*malloc_count_ + time)/(malloc_count_ + 1);
            malloc_count_ += 1;

        }
        for(int i = 0; i < rand_iter; i ++){
            if(rand()%100 > 20 && addr[i] != 0){
                if(!alloc->free(addr[i]))
                    printf("free error!\n");
                    addr[i] = 0;
                    rkey[i] = 0;
        }
    }
    }
    for(int i=0;i<1000;i++){
        malloc_record_global[i].fetch_add(malloc_record[i]);
        free_record_global[i].fetch_add(free_record[i]);
    }
    malloc_avg.fetch_add(malloc_avg_time_);
    free_avg.fetch_add(free_avg_time_);
}

void short_alloc(mralloc::ConnectionManager* conn, test_allocator* alloc, uint64_t thread_id) {
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
        for(int i = 0; i < rand_iter; i ++){
            if(!alloc->malloc(addr, rkey)|| addr == 0){
                printf("alloc false\n");
            }
            if(!alloc->free(addr)){
                printf("alloc false\n");
            }
        }
        gettimeofday(&end, NULL);
        pthread_barrier_wait(&end_barrier);
        printf("epoch %d malloc finish\n", j);
        
        if (thread_id == 1)
            conn->remote_print_alloc_info();
            
        uint64_t time =  end.tv_usec + end.tv_sec*1000*1000 - start.tv_usec - start.tv_sec*1000*1000;
        time = time / rand_iter;
        if(time < 1000)
            malloc_record[time] += 1;
        malloc_avg_time_ = (malloc_avg_time_*malloc_count_ + time)/(malloc_count_ + 1);
        malloc_count_ += 1;
        // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
    }
    for(int i=0;i<1000;i++){
        malloc_record_global[i].fetch_add(malloc_record[i]);
    }
    malloc_avg.fetch_add(malloc_avg_time_);
}

void* worker(void* arg) {
    uint64_t thread_id = id.fetch_add(1);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    int id_ = id+3;
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
    test_allocator* alloc;
    switch (type)
    {
    case cxl_shm_alloc:
        alloc = (test_allocator*)new cxl_shm_allocator(conn, 0);
        break;
    case fusee_alloc:
        alloc = (test_allocator*)new fusee_allocator(conn);
        break;
    case rpc_alloc:
        alloc = (test_allocator*)new rpc_allocator(conn);
        break;
    case share_alloc:
        alloc = (test_allocator*)new share_allocator(conn);
        break;
    case exclusive_alloc:
        alloc = (test_allocator*)new exclusive_allocator(conn);
        break;
    case pool_alloc:
        alloc = (test_allocator*)new pool_allocator();
        break;
    default:
        break;
    }
    pthread_barrier_wait(&start_barrier);
    warmup(alloc);
    pthread_barrier_wait(&end_barrier);
    pthread_barrier_wait(&start_barrier);
    // shuffle_alloc(conn, alloc, thread_id);
    switch (test)
    {
    case stage_test:
        stage_alloc(conn, alloc, thread_id);
        break;
    case shuffle_test:
        shuffle_alloc(conn, alloc, thread_id);
        break;
    case short_test:
        short_alloc(conn, alloc, thread_id);
        break;
    default:
        break;
    }
    pthread_barrier_wait(&end_barrier);
    return NULL;
}

int main(int argc, char* argv[]) {
    if(argc < 6){
        printf("Usage: %s <ip> <port> <thread> <allocator> <trace>\n", argv[0]);
        return 0;
    }

    std::string ip = argv[1];
    std::string port = argv[2];
    int thread_num = atoi(argv[3]);
    std::string allocator_type = argv[4];
    std::string trace_type = argv[5];
    if (allocator_type == "cxl")
        type = cxl_shm_alloc;
    else if (allocator_type == "fusee") 
        type = fusee_alloc;
    else if (allocator_type == "rpc")
        type = rpc_alloc;
    else if (allocator_type == "share")
        type = share_alloc;
    else if (allocator_type == "exclusive")
        type = exclusive_alloc;
    else if (allocator_type == "pool")
        type = pool_alloc;
    else {
        printf("allocator type error\n");
        return -1;
    }

    if (trace_type == "stage")
        test = stage_test;
    else if (trace_type == "shuffle")
        test = shuffle_test;
    else if (trace_type == "short")
        test = short_test;
    else {
        printf("test type error\n");
        return -1;
    }

    std::ofstream result;
    result.open("result_" + allocator_type + "_"  + trace_type + "_.csv");
    
    for(int i=0;i<1000;i++){
        malloc_record_global[i].store(0);
        free_record_global[i].store(0);
    }
    id.store(0);
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
    printf("total malloc avg: %luus\n", malloc_avg.load()/thread_num);
    result << "total malloc avg: " << malloc_avg.load()/thread_num << std::endl;
    printf("total free avg: %luus\n", free_avg.load()/thread_num);
    result << "total free avg: " << free_avg.load()/thread_num << std::endl;
    result.close();
}
