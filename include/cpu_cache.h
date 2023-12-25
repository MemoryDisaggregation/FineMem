

#include <bits/stdint-uintn.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>

#pragma once

namespace mralloc {

const uint32_t nprocs = 144;
const uint32_t max_alloc_item = 256;
const uint32_t max_free_item = 256;
const uint32_t max_class_free_item = 512;

template <typename T>
class ring_buffer{
public:
    ring_buffer(uint32_t max_length, T* buffer, T zero, uint32_t* reader, uint32_t* writer)
         : max_length_(max_length), buffer_(buffer), zero_(zero), reader_(reader), writer_(writer) {
    }
    
    void clear() {
        for(int i = 0; i < max_length_; i++) {
            buffer_[i] = zero_;
        }
        *reader_=0;
        *writer_=0;
    }

    uint32_t get_length() {
        uint32_t reader = *reader_;
        uint32_t writer = *writer_;
        if (reader == writer) {
            return 0;
        }
        if (writer > reader) {
            return (writer - reader);
        } else {
            return (max_length_ - reader + writer);
        }
    }

    void add_cache(T value){
        // host side fill cache, add write pointer
        uint32_t writer = *writer_;
        if(get_length() < max_length_-1){
            buffer_[writer] = value;        
            *writer_ = (writer + 1) % max_length_;
        }
    }

    void add_batch(T* value, uint64_t num) {
        uint32_t writer = *writer_;
        if(get_length() < max_length_-num){
            for(int i = 0; i < num; i++){
                buffer_[(writer + i) % max_length_] = value[i];        
            }
            *writer_ = (writer + num) % max_length_;
        }
    }

    bool force_fetch_cache(T &value) {
        uint32_t reader = *reader_;
        while(get_length() == 0 ) ;
        *reader_ = (reader + 1) % max_length_;
        while(!(buffer_[reader] != zero_)){
            reader = *reader_;
        }
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

    bool try_fetch_cache(T &value) {
        uint32_t reader = *reader_;
        if(get_length() == 0) {
            return false;
        } 
        *reader_ = (reader + 1) % max_length_;
        while(!(buffer_[reader] != zero_)){
            reader = *reader_;
        }
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

    bool try_fetch_batch(T* value, uint64_t num) {
        uint64_t length = get_length();
        uint32_t reader = *reader_;
        if(length < num) {
            return false;
        } 
        *reader_ = (reader + num) % max_length_;
        for(int i = 0; i< num;i++) {
            while(!(buffer_[(reader + i) % max_length_] != zero_));
            value[i] = buffer_[(reader + i) % max_length_];
            buffer_[(reader + i) % max_length_] = zero_;
        }
        return true;
    }

    int try_fetch_batch_all(T *value) {
        uint64_t length = get_length();
        uint32_t reader = *reader_;
        if(length == 0) {
            return 0;
        } 
        *reader_ = (reader + length) % max_length_;
        for(int i = 0; i< length;i++) {
            while(!(buffer_[(reader + i) % max_length_] != zero_));
            value[i] = buffer_[(reader + i) % max_length_];
            buffer_[(reader + i) % max_length_] = zero_;
        }
        return length;
    }

private:
    uint32_t max_length_;
    T* buffer_;
    T zero_;
    uint32_t* reader_;
    uint32_t* writer_;
};

template <typename T>
class ring_buffer_atomic{
public:
    ring_buffer_atomic(uint32_t max_length, T* buffer, T zero, std::atomic<uint32_t>* reader, std::atomic<uint32_t>* writer)
         : max_length_(max_length), buffer_(buffer), zero_(zero), reader_(reader), writer_(writer) {
    }
    
    void clear() {
        for(int i = 0; i < max_length_; i++) {
            buffer_[i] = zero_;
        }
        reader_->store(0);
        writer_->store(0);
    }

    int get_length() {
        uint32_t reader = reader_->load()%max_length_;
        uint32_t writer = writer_->load()%max_length_;
        if(!(buffer_[reader] != zero_))
            return 0;
        if (reader == writer) {
            return 0;
        }
        if (writer > reader) {
            return (writer - reader);
        } else {
            return (max_length_ - reader + writer);
        }
    }

    void add_cache(T value){
        // host side fill cache, add write pointer
        uint32_t writer;
        if(get_length() < max_length_-1){
            writer = writer_->fetch_add(1)%max_length_;
        } else {
            return;
        }
        buffer_[writer] = value;
    }

    void add_batch(T* value, uint64_t num) {
        uint32_t writer;
        if(get_length() < max_length_-num) {
            writer = writer_->fetch_add(num);
            for(int i = 0; i < num; i++){
                buffer_[(writer + i) % max_length_] = value[i];        
            }
        }
    }

    bool force_fetch_cache(T &value) {
        uint32_t reader = 0;
        reader = reader_->fetch_add(1)%max_length_;
        while(!(buffer_[reader] != zero_));
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

    bool try_fetch_batch(T* value, uint64_t num) {
        uint32_t reader = reader_->load();
        uint32_t new_reader;
        do {
            if(get_length() < num) {
                return false;
            } 
            new_reader = (reader + num) % max_length_;
        }while(!reader_->compare_exchange_strong(reader, new_reader));
        for(int i = 0; i < num; i++){
            while(!(buffer_[(reader + i) % max_length_] != zero_));
            value[i] = buffer_[(reader + i) % max_length_];
            buffer_[(reader + i) % max_length_] = zero_;
        }
        return true;
    }

    int try_fetch_batch_all(T *value) {
        uint64_t length = get_length();
        if(length == 0) {
            return 0;
        } 
        uint32_t reader = reader_->load();
        uint32_t new_reader;
        do {
            new_reader = (reader + length) % max_length_;
        }while(!reader_->compare_exchange_strong(reader, new_reader));
        for(int i = 0; i< length; i++) {
            while(!(buffer_[(reader + i) % max_length_] != zero_));
            value[i] = buffer_[(reader + i) % max_length_];
            buffer_[(reader + i) % max_length_] = zero_;
        }
        return length;
    }

    bool try_fetch_cache(T &value) {
        uint32_t reader = reader_->load();
        uint32_t new_reader;
        do {
            if(get_length() == 0) {
                return false;
            } 
            new_reader = (reader + 1) % max_length_;
        }while(!reader_->compare_exchange_strong(reader, new_reader));
        while(!(buffer_[reader] != zero_));
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

private:
    uint32_t max_length_;
    T* buffer_;
    T zero_;
    std::atomic<uint32_t>* reader_;
    std::atomic<uint32_t>* writer_;
};

struct mr_rdma_addr_{
    uint64_t addr;
    uint32_t rkey;
};

class mr_rdma_addr{
public:
    mr_rdma_addr() {addr = -1; rkey = -1;}
    mr_rdma_addr(uint64_t addr, uint32_t rkey): addr(addr), rkey(rkey) {}
    bool operator==(mr_rdma_addr &compare) {
        return addr == compare.addr && rkey == compare.rkey;
    }
    bool operator!=(mr_rdma_addr &compare) {
        return addr != compare.addr && rkey != compare.rkey;
    }
    mr_rdma_addr& operator=(mr_rdma_addr &value) {
        this->addr = value.addr;
        this->rkey = value.rkey;
        return *this;
    }
    uint64_t addr;
    uint32_t rkey;
} ;

const uint32_t class_num = 16;

class cpu_cache{
public:

    struct cpu_cache_storage {
        uint64_t block_size;
        mr_rdma_addr items[nprocs][max_alloc_item];
        uint64_t free_items[nprocs][max_free_item];
        mr_rdma_addr class_items[class_num][max_alloc_item];
        uint64_t class_free_items[class_num][max_class_free_item];

        uint32_t reader[nprocs];
        std::atomic<uint32_t> class_reader[class_num];
        uint32_t free_reader[nprocs];
        std::atomic<uint32_t> class_free_reader[nprocs];

        uint32_t writer[nprocs];
        std::atomic<uint32_t> class_writer[class_num];
        uint32_t free_writer[nprocs];
        std::atomic<uint32_t> class_free_writer[nprocs];
    };

    cpu_cache() {
        this->null = {0,0};
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if (fd == -1) {
            perror("init failed, no computing node running");
        } else {
            int port_flag = PROT_READ | PROT_WRITE;
            int mm_flag   = MAP_SHARED; 
            cpu_cache_content_ = (cpu_cache_storage*)mmap(NULL, sizeof(cpu_cache_storage), port_flag, mm_flag, fd, 0);
            cache_size_ = cpu_cache_content_->block_size;
            for(int i = 0; i < nprocs; i++) {
                alloc_ring[i] = new ring_buffer<mr_rdma_addr>(max_alloc_item, cpu_cache_content_->items[i], mr_rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer_atomic<mr_rdma_addr>(max_alloc_item, cpu_cache_content_->class_items[i], mr_rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_free_ring[i] = new ring_buffer_atomic<uint64_t>(max_class_free_item, cpu_cache_content_->class_free_items[i], -1, 
                    &cpu_cache_content_->class_free_reader[i], &cpu_cache_content_->class_free_writer[i]);
            }
        }
        
    }

    cpu_cache(uint64_t cache_size) : cache_size_(cache_size) {
        int port_flag = PROT_READ | PROT_WRITE;
        int mm_flag   = MAP_SHARED; 
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if(fd==-1){
            // if shm not exist, create and init it
            fd = shm_open("/cpu_cache", O_CREAT | O_EXCL | O_RDWR, 0600);
            // TODO: our cpu number should be dynamic, max_item can be static
            // e.g. const uint8_t nprocs = get_nprocs();
            ftruncate(fd, sizeof(cpu_cache_storage));
            cpu_cache_content_ = (cpu_cache_storage*)mmap(NULL, sizeof(cpu_cache_storage), port_flag, mm_flag, fd, 0);
            for(int i = 0; i < nprocs; i++) {
                alloc_ring[i] = new ring_buffer<mr_rdma_addr>(max_alloc_item, cpu_cache_content_->items[i], mr_rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                alloc_ring[i]->clear();

                free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                free_ring[i]->clear();
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer_atomic<mr_rdma_addr>(max_alloc_item, cpu_cache_content_->class_items[i], mr_rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_ring[i]->clear();

                class_free_ring[i] = new ring_buffer_atomic<uint64_t>(max_class_free_item, cpu_cache_content_->class_free_items[i], -1, 
                    &cpu_cache_content_->class_free_reader[i], &cpu_cache_content_->class_free_writer[i]);
                class_free_ring[i]->clear();
            }
            cpu_cache_content_->block_size = cache_size_;
        }
        else {
            cpu_cache_content_ = (cpu_cache_storage*)mmap(NULL, sizeof(cpu_cache_storage), port_flag, mm_flag, fd, 0);
            assert(cache_size_ == cpu_cache_content_->block_size);
            cache_size_ = cpu_cache_content_->block_size;
            for(int i = 0; i < nprocs; i++) {
                alloc_ring[i] = new ring_buffer<mr_rdma_addr>(max_alloc_item, cpu_cache_content_->items[i], mr_rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer_atomic<mr_rdma_addr>(max_alloc_item, cpu_cache_content_->class_items[i], mr_rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_free_ring[i] = new ring_buffer_atomic<uint64_t>(max_class_free_item, cpu_cache_content_->class_free_items[i], -1, 
                    &cpu_cache_content_->class_free_reader[i], &cpu_cache_content_->class_free_writer[i]);
            }
        }
    }

    ~cpu_cache(){
        if(cpu_cache_content_)
            munmap(cpu_cache_content_, sizeof(cpu_cache_storage));
    }

    uint64_t get_cache_size(){
        // cache size maybe different, but static
        return cache_size_;
    }

    void free_cache(){
        // only the global host side need call this, to free all cpu_cache 
        if(cpu_cache_content_)
            munmap(cpu_cache_content_, sizeof(cpu_cache_storage));
        shm_unlink("/cpu_cache");
        for(int i = 0; i < nprocs; i++) {
            delete(alloc_ring[i]);
            delete(free_ring[i]);
        }
        for(int i = 0; i < class_num; i++) {
            delete(class_ring[i]);
        }
    }

    bool fetch_cache(uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        unsigned nproc;
        if((nproc = sched_getcpu()) == -1){
            printf("sched_getcpu bad \n");
            return false;
        }
        mr_rdma_addr result(-1, -1);
        bool ret = alloc_ring[nproc]->force_fetch_cache(result);
        addr = result.addr; rkey = result.rkey;
        return ret;
    }

    bool fetch_cache(uint32_t nproc, uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        mr_rdma_addr result(-1, -1);
        bool ret = alloc_ring[nproc]->force_fetch_cache(result);
        addr = result.addr; rkey = result.rkey;
        return ret;
    }

    uint64_t fetch_free_cache(uint32_t nproc, uint64_t* addr) {
        return free_ring[nproc]->try_fetch_batch_all(addr);
    }

    bool fetch_class_cache(uint16_t block_class, uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        mr_rdma_addr result(-1, -1);
        bool ret = class_ring[block_class]->force_fetch_cache(result);
        addr = result.addr; rkey = result.rkey;
        return ret;
    }

    uint64_t fetch_class_free_cache(uint32_t nproc, uint64_t* addr) {
        return class_free_ring[nproc]->try_fetch_batch_all(addr);
    }

    void add_cache(uint32_t nproc, uint64_t addr, uint32_t rkey){
        // host side fill cache, add write pointer
        mr_rdma_addr result(addr, rkey);
        alloc_ring[nproc]->add_cache(result);
    }

    void add_batch(uint32_t nproc, mr_rdma_addr* value, uint64_t num){
        // host side fill cache, add write pointer
        alloc_ring[nproc]->add_batch(value, num);
    }

    void add_class_batch(uint32_t class_, mr_rdma_addr* value, uint64_t num){
        // host side fill cache, add write pointer
        class_ring[class_]->add_batch(value, num);
    }

    void add_free_cache(uint64_t addr) {
        unsigned nproc;
        if((nproc = sched_getcpu()) == -1){
            printf("sched_getcpu bad \n");
            return;
        }
        free_ring[nproc]->add_cache(addr);
    }

    void add_class_cache(uint16_t block_class, uint64_t addr, uint32_t rkey) {
        mr_rdma_addr result(addr, rkey);
        class_ring[block_class]->add_cache(result);
    }

    void add_class_free_cache(uint16_t block_class, uint64_t addr) {
        class_free_ring[block_class]->add_cache(addr);
    }

    inline uint32_t get_length(uint32_t nproc) {
        return alloc_ring[nproc]->get_length();
    }

    inline uint32_t get_free_length(uint32_t nproc) {
        return free_ring[nproc]->get_length();
    }

    inline uint32_t get_class_length(uint32_t nproc) {
        return class_ring[nproc]->get_length();
    }
    
    inline uint32_t get_class_free_length(uint32_t nproc) {
        return class_ring[nproc]->get_length();
    }

private:
    cpu_cache_storage* cpu_cache_content_;
    uint64_t cache_size_;
    ring_buffer<mr_rdma_addr>* alloc_ring[nprocs];
    ring_buffer_atomic<mr_rdma_addr>* class_ring[nprocs];
    ring_buffer<uint64_t>* free_ring[nprocs];
    ring_buffer_atomic<uint64_t>* class_free_ring[nprocs];
    mr_rdma_addr_ null;
};

}
