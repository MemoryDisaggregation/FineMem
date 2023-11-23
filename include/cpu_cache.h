

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

class cpu_cache{
public:
    struct item {
        uint64_t addr;
        uint32_t rkey;
    };

    struct cpu_cache_storage {
        uint64_t block_size;
        item items[nprocs][max_item];
        uint32_t reader[nprocs];
        uint32_t writer[nprocs];
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
            for (int i = 0; i < nprocs; i++) {
                cpu_cache_content_->reader[i] = 0;
                cpu_cache_content_->writer[i] = 0;
                for(int j = 0; j < max_item; j++) {
                    cpu_cache_content_->items[i][j].addr = -1;
                    cpu_cache_content_->items[i][j].rkey = 0;
                    // content_[i * max_item + j].addr = -1;
                }
            }
            cpu_cache_content_->block_size = cache_size_;
        }
        else {
            cpu_cache_content_ = (cpu_cache_storage*)mmap(NULL, sizeof(cpu_cache_storage), port_flag, mm_flag, fd, 0);
            assert(cache_size_ == cpu_cache_content_->block_size);
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
        unsigned cpu; unsigned nproc;
        if(getcpu(&cpu, &nproc) == -1){
            printf("getcpu bad \n");
            return false;
        }
        while(cpu_cache_content_->reader[nproc] == cpu_cache_content_->writer[nproc]) ;
        uint32_t reader = cpu_cache_content_->reader[nproc];
        item fetch_one = cpu_cache_content_->items[nproc][reader];
        if(fetch_one.addr != -1 && fetch_one.rkey != 0) {
            addr = fetch_one.addr;
            rkey = fetch_one.rkey;
            cpu_cache_content_->items[nproc][reader].addr = -1;
            cpu_cache_content_->items[nproc][reader].rkey = 0;
            cpu_cache_content_->reader[nproc] = (reader + 1) % max_item;
            return true;
        }
        else{
            return false;
        }
    }

    bool fetch_cache(uint32_t nproc, uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        while(cpu_cache_content_->reader[nproc] == cpu_cache_content_->writer[nproc]) ;
        uint32_t reader = cpu_cache_content_->reader[nproc];
        item fetch_one = cpu_cache_content_->items[nproc][reader];
        if(fetch_one.addr != -1 && fetch_one.rkey != 0) {
            addr = fetch_one.addr;
            rkey = fetch_one.rkey;
            cpu_cache_content_->items[nproc][reader].addr = -1;
            cpu_cache_content_->items[nproc][reader].rkey = 0;
            cpu_cache_content_->reader[nproc] = (reader + 1) % max_item;
            return true;
        }
        else{
            return false;
        }
    }

    void add_cache(uint32_t nproc, uint64_t addr, uint32_t rkey){
        // host side fill cache, add write pointer
        uint32_t writer = cpu_cache_content_->writer[nproc];
        if(cpu_cache_content_->items[nproc][writer].addr == -1){
            cpu_cache_content_->items[nproc][writer].rkey = rkey;
            cpu_cache_content_->items[nproc][writer].addr = addr;
            cpu_cache_content_->writer[nproc] = (writer + 1) % max_item;
        }
    }

    uint32_t get_length(uint32_t nproc) {
        uint32_t writer = cpu_cache_content_->writer[nproc];
        uint32_t reader = cpu_cache_content_->reader[nproc];
        if (writer == reader) {
            if (cpu_cache_content_->items[nproc][writer].addr == -1){
                return 0;
            } else {
                return max_item;
            }
        }
        if (writer > reader) {
            return (writer - reader);
        } else {
            return (max_item - reader + writer);
        }
    }
    
private:
    cpu_cache_storage* cpu_cache_content_;
    uint64_t cache_size_;
};

}