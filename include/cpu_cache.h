

#include <bits/stdint-uintn.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>

namespace mralloc {

const uint32_t nprocs = 80;
const uint32_t max_item = 1024;

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
        *reader_ = 0;
        *writer_ = 0;
    }

    uint32_t get_length() {
        if (*reader_ == *writer_) {
            return 0;
        }
        if (*writer_ > *reader_) {
            return (*writer_ - *reader_);
        } else {
            return (max_length_ - *reader_ + *writer_);
        }
    }

    void add_cache(T value){
        // host side fill cache, add write pointer
        uint32_t writer = *writer_;
        if(get_length() < max_length_-1 && buffer_[writer] == zero_){
            buffer_[writer] = value;
            *writer_ = (writer + 1) % max_item;
        }
    }

    bool force_fetch_cache(T &value) {
        while(get_length() == 0) ;
        uint32_t reader = *reader_;
        if(!(buffer_[reader] == zero_ )) {
            value = buffer_[reader];
            buffer_[reader] = zero_;
            *reader_ = (reader + 1) % max_item;
            return true;
        }
        else{
            printf("fetch cache failed\n");
            return false;
        }
    }

    bool try_fetch_cache(T &value) {
        if(get_length() == 0) {
            return false;
        } 
        uint32_t reader = *reader_;
        if(!(buffer_[reader] == zero_ )) {
            value = buffer_[reader];
            buffer_[reader] = zero_;
            *reader_ = (reader + 1) % max_item;
            return true;
        }
        else{
            printf("fetch cache failed\n");
            return false;
        }
    }

private:
    uint32_t max_length_;
    T* buffer_;
    T zero_;
    uint32_t* reader_;
    uint32_t* writer_;
};

class rdma_addr{
public:
    rdma_addr(uint64_t addr, uint32_t rkey): addr(addr), rkey(rkey) {}
    bool operator==(rdma_addr &compare) {
        return addr == compare.addr && rkey == compare.rkey;
    }
    rdma_addr& operator=(rdma_addr &value) {
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
        rdma_addr items[nprocs][max_item];
        uint64_t free_items[nprocs][max_item];
        rdma_addr class_items[class_num][max_item];

        uint32_t reader[nprocs];
        uint32_t class_reader[class_num];
        uint32_t free_reader[nprocs];

        uint32_t writer[nprocs];
        uint32_t class_writer[class_num];
        uint32_t free_writer[nprocs];
    };

    cpu_cache() {
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if (fd == -1) {
            perror("init failed, no computing node running");
        } else {
            int port_flag = PROT_READ | PROT_WRITE;
            int mm_flag   = MAP_SHARED; 
            cpu_cache_content_ = (cpu_cache_storage*)mmap(NULL, sizeof(cpu_cache_storage), port_flag, mm_flag, fd, 0);
            cache_size_ = cpu_cache_content_->block_size;
            for(int i = 0; i < nprocs; i++) {
                alloc_ring[i] = new ring_buffer<rdma_addr>(max_item, cpu_cache_content_->items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                free_ring[i] = new ring_buffer<uint64_t>(max_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer<rdma_addr>(max_item, cpu_cache_content_->class_items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
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
                alloc_ring[i] = new ring_buffer<rdma_addr>(max_item, cpu_cache_content_->items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                alloc_ring[i]->clear();
                free_ring[i] = new ring_buffer<uint64_t>(max_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                alloc_ring[i]->clear();
                free_ring[i]->clear();
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer<rdma_addr>(max_item, cpu_cache_content_->class_items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_ring[i]->clear();
            }
            cpu_cache_content_->block_size = cache_size_;
        }
        else {
            cpu_cache_content_ = (cpu_cache_storage*)mmap(NULL, sizeof(cpu_cache_storage), port_flag, mm_flag, fd, 0);
            assert(cache_size_ == cpu_cache_content_->block_size);
            cache_size_ = cpu_cache_content_->block_size;
            for(int i = 0; i < nprocs; i++) {
                alloc_ring[i] = new ring_buffer<rdma_addr>(max_item, cpu_cache_content_->items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                free_ring[i] = new ring_buffer<uint64_t>(max_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer<rdma_addr>(max_item, cpu_cache_content_->class_items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
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
    }

    bool fetch_cache(uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        unsigned nproc;
        if((nproc = sched_getcpu()) == -1){
            printf("sched_getcpu bad \n");
            return false;
        }
        rdma_addr result(-1, -1);
        bool ret = alloc_ring[nproc]->force_fetch_cache(result);
        addr = result.addr; rkey = result.rkey;
        return ret;
    }

    bool fetch_cache(uint32_t nproc, uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        rdma_addr result(-1, -1);
        bool ret = alloc_ring[nproc]->force_fetch_cache(result);
        addr = result.addr; rkey = result.rkey;
        return ret;
    }

    void add_cache(uint32_t nproc, uint64_t addr, uint32_t rkey){
        // host side fill cache, add write pointer
        rdma_addr result(addr, rkey);
        alloc_ring[nproc]->add_cache(result);
    }

    void add_free_cache(uint64_t addr) {
        unsigned nproc;
        if((nproc = sched_getcpu()) == -1){
            printf("sched_getcpu bad \n");
            return;
        }
        free_ring[nproc]->add_cache(addr);
    }

    bool fetch_free_cache(uint32_t nproc, uint64_t &addr) {
        return free_ring[nproc]->try_fetch_cache(addr);
    }

    inline uint32_t get_length(uint32_t nproc) {
        return alloc_ring[nproc]->get_length();
    }

    inline uint32_t get_free_length(uint32_t nproc) {
        return free_ring[nproc]->get_length();
    }
    
private:
    cpu_cache_storage* cpu_cache_content_;
    uint64_t cache_size_;
    ring_buffer<rdma_addr>* alloc_ring[nprocs];
    ring_buffer<rdma_addr>* class_ring[nprocs];
    ring_buffer<uint64_t>* free_ring[nprocs];
};

}