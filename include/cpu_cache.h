

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

namespace mralloc {

const uint32_t nprocs = 80;
const uint32_t max_alloc_item = 16;
const uint32_t max_free_item = 256;

template <typename T>
class ring_buffer{
public:
    ring_buffer(uint32_t max_length, T* buffer, T zero, std::atomic<uint32_t>* reader, std::atomic<uint32_t>* writer)
         : max_length_(max_length), buffer_(buffer), zero_(zero), reader_(reader), writer_(writer) {
    }
    
    void clear() {
        for(int i = 0; i < max_length_; i++) {
            buffer_[i] = zero_;
        }
        reader_->store(0);
        writer_->store(0);
    }

    uint32_t get_length() {
        uint32_t reader = reader_->load();
        uint32_t writer = writer_->load();
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
        uint32_t writer = writer_->load();
        uint32_t new_writer;
        do{
            new_writer = writer;
            if(get_length() < max_length_-1){
                new_writer = (new_writer + 1) % max_length_;
            } else {
                return;
            }
        } while(!writer_->compare_exchange_strong(writer, new_writer));
        buffer_[writer] = value;
    }

    bool force_fetch_cache(T &value) {
        uint32_t reader = reader_->load();
        uint32_t new_reader;
        do {
            while(get_length() == 0) ;
            new_reader = (reader + 1) % max_length_;
            // if(!(buffer_[reader] == zero_ )) {
            //     new_reader = (reader + 1) % max_length_;
            // }
            // else{
            //     printf("fetch cache failed\n");
            //     return false;
            // }
        }while(!reader_->compare_exchange_strong(reader, new_reader));
        while(buffer_[reader] == zero_);
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

    bool try_fetch_cache(T &value) {
        uint32_t reader = reader_->load();
        uint32_t new_reader;
        do {
            if(get_length() == 0) {
                return false;
            } 
            new_reader = (reader + 1) % max_length_;
            // if(!(buffer_[reader] == zero_ )) {
            //     new_reader = (reader + 1) % max_length_;
            // }
            // else{
            //     printf("fetch cache failed\n");
            //     return false;
            // }
        }while(!reader_->compare_exchange_strong(reader, new_reader));
        while(buffer_[reader] == zero_);
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

struct rdma_addr_{
    uint64_t addr;
    uint32_t rkey;
};

class rdma_addr{
public:
    rdma_addr() {addr = -1; rkey = -1;}
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
        rdma_addr items[nprocs][max_alloc_item];
        uint64_t free_items[nprocs][max_free_item];
        rdma_addr class_items[class_num][max_alloc_item];
        uint64_t class_free_items[class_num][max_free_item];

        std::atomic<uint32_t> reader[nprocs];
        std::atomic<uint32_t> class_reader[class_num];
        std::atomic<uint32_t> free_reader[nprocs];
        std::atomic<uint32_t> class_free_reader[nprocs];

        std::atomic<uint32_t> writer[nprocs];
        std::atomic<uint32_t> class_writer[class_num];
        std::atomic<uint32_t> free_writer[nprocs];
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
                alloc_ring[i] = new ring_buffer<rdma_addr>(max_alloc_item, cpu_cache_content_->items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer<rdma_addr>(max_alloc_item, cpu_cache_content_->class_items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->class_free_items[i], -1, 
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
                alloc_ring[i] = new ring_buffer<rdma_addr>(max_alloc_item, cpu_cache_content_->items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                alloc_ring[i]->clear();

                free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                free_ring[i]->clear();
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer<rdma_addr>(max_alloc_item, cpu_cache_content_->class_items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_ring[i]->clear();

                class_free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->class_free_items[i], -1, 
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
                alloc_ring[i] = new ring_buffer<rdma_addr>(max_alloc_item, cpu_cache_content_->items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->reader[i], &cpu_cache_content_->writer[i]);
                free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->free_items[i], -1, 
                    &cpu_cache_content_->free_reader[i], &cpu_cache_content_->free_writer[i]);
                
            }
            for(int i = 0; i < class_num; i++) {
                class_ring[i] = new ring_buffer<rdma_addr>(max_alloc_item, cpu_cache_content_->class_items[i], rdma_addr(-1,-1), 
                    &cpu_cache_content_->class_reader[i], &cpu_cache_content_->class_writer[i]);
                class_free_ring[i] = new ring_buffer<uint64_t>(max_free_item, cpu_cache_content_->class_free_items[i], -1, 
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

    bool fetch_free_cache(uint32_t nproc, uint64_t &addr) {
        return free_ring[nproc]->try_fetch_cache(addr);
    }

    bool fetch_class_cache(uint16_t block_class, uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        rdma_addr result(-1, -1);
        bool ret = class_ring[block_class]->force_fetch_cache(result);
        addr = result.addr; rkey = result.rkey;
        return ret;
    }

    bool fetch_class_free_cache(uint32_t nproc, uint64_t &addr) {
        return class_free_ring[nproc]->try_fetch_cache(addr);
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

    void add_class_cache(uint16_t block_class, uint64_t addr, uint32_t rkey) {
        rdma_addr result(addr, rkey);
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
    ring_buffer<rdma_addr>* alloc_ring[nprocs];
    ring_buffer<rdma_addr>* class_ring[nprocs];
    ring_buffer<uint64_t>* free_ring[nprocs];
    ring_buffer<uint64_t>* class_free_ring[nprocs];
    rdma_addr_ null;
};

}