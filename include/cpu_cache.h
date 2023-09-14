

#include <bits/stdint-uintn.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>

namespace mralloc {

const uint32_t nprocs = 32;
const uint32_t max_item = 1024;

class cpu_cache{
public:
    struct item {
        uint64_t addr;
        uint32_t rkey;
    };
    cpu_cache(uint64_t cache_size) : cache_size_(cache_size) {
        // init cache at host/local side
        // try to open the cpu cache shared memory
        int port_flag = PROT_READ | PROT_WRITE;
        int mm_flag   = MAP_SHARED; 
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if(fd==-1){
            // if shm not exist, create and init it
            fd = shm_open("/cpu_cache", O_CREAT | O_EXCL | O_RDWR, 0600);
            // change size to cpu number * max_item
            // TODO: our cpu number should be dynamic, max_item can be static
            // e.g. const uint8_t nprocs = get_nprocs();
            ftruncate(fd, sizeof(item) * nprocs * (max_item) + 2 * sizeof(uint32_t) *nprocs);
            shared_cpu_cache_ = (uint64_t*)mmap(NULL, sizeof(item) * nprocs * (max_item) + 2 * sizeof(uint32_t) * nprocs, port_flag, mm_flag, fd, 0);
            // ring buffer read and write pointer
            read_p_ = (uint32_t*)shared_cpu_cache_;
            write_p_ = (uint32_t*)shared_cpu_cache_ + nprocs;
            content_ = (item*)(shared_cpu_cache_ + nprocs);
            // init all cpu ring buffer with -1
            for (int i = 0; i < nprocs; i++) {
                read_p_[i] = 0;
                write_p_[i] = 0;
                for(int j = 0; j < max_item; j++) {
                    content_[i * max_item + j].addr = -1;
                }
            }
        }
        else {
            // open shm success, then mmap this file to local memory, and get ring buffer read and write 
            shared_cpu_cache_ = (uint64_t*)mmap(NULL, sizeof(item) * nprocs * (max_item) + 2 * sizeof(uint32_t) * nprocs, port_flag, mm_flag, fd, 0);
            read_p_ = (uint32_t*)shared_cpu_cache_;
            write_p_ = (uint32_t*)shared_cpu_cache_ + nprocs;
            content_ = (item*)(shared_cpu_cache_ + nprocs);
        }
    }

    ~cpu_cache(){
        if(shared_cpu_cache_)
            munmap(shared_cpu_cache_, sizeof(item) * nprocs * (max_item) + 2 * sizeof(uint32_t) * nprocs);
    }

    uint64_t get_cache_size(){
        // cache size maybe different, but static
        return cache_size_;
    }

    void free_cache(){
        // only the global host side need call this, to free all cpu_cache 
        if(shared_cpu_cache_)
            munmap(shared_cpu_cache_, sizeof(item) * nprocs * (max_item) + 2 * sizeof(uint32_t) * nprocs);
        shm_unlink("/cpu_cache");
    }

    bool fetch_cache(uint32_t nproc, uint64_t &addr, uint32_t &rkey){
        // just fetch one block in the current cpu_id --> ring buffer
        item fetch_one = content_[nproc * max_item + read_p_[nproc]];
        if(fetch_one.addr != -1) {
            content_[nproc * max_item + read_p_[nproc]].addr = -1;
            read_p_[nproc] = (read_p_[nproc] + 1) % max_item;
            addr = fetch_one.addr;
            rkey = fetch_one.rkey;
            return true;
        }
        else{
            return false;
        }
    }

    void return_cache(uint32_t nproc, uint64_t addr, uint32_t rkey){
        // using by local side, use to back a block at read_pointer as a fast return path
        uint32_t prev = (read_p_[nproc] - 1) % max_item;
        if(content_[nproc * max_item + prev].addr == -1){
            content_[nproc * max_item + prev] = {addr, rkey};
            read_p_[nproc] = prev;
        }
    }

    void add_cache(uint32_t nproc, uint64_t addr, uint32_t rkey){
        // host side fill cache, add write pointer
        if(content_[nproc * max_item + write_p_[nproc]].addr == -1){
            content_[nproc * max_item + write_p_[nproc]] = {addr, rkey};
            write_p_[nproc] = (write_p_[nproc] + 1) % max_item;
        }
    }

    bool is_full(uint32_t nproc){
        return (write_p_[nproc] == read_p_[nproc] && content_[nproc * max_item + write_p_[nproc]].addr != -1);
    }

    bool is_empty(uint32_t nproc){
        return (write_p_[nproc] == read_p_[nproc] && content_[nproc * max_item + write_p_[nproc]].addr == -1);
    }

private:
    // uint64_t *base_addr_;
    uint64_t *shared_cpu_cache_;
    item *content_;
    uint32_t *read_p_;
    uint32_t *write_p_;
    uint64_t cache_size_;
};

}