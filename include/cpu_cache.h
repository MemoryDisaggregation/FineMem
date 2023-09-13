

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

const uint32_t nprocs = 2;
const uint32_t max_item = 1024;

class cpu_cache{
public:
    cpu_cache(uint64_t cache_size) : cache_size_(cache_size) {
        // init cache at host/local side
        // try to open the cpu cache shared memory
        int port_flag = PROT_READ | PROT_WRITE;
        int mm_flag   = MAP_SHARED | MAP_FIXED; 
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if(fd==-1){
            // if shm not exist, create and init it
            fd = shm_open("/cpu_cache", O_CREAT | O_EXCL | O_RDWR, 0777);
            // change size to cpu number * max_item
            // TODO: our cpu number should be dynamic, max_item can be static
            // e.g. const uint8_t nprocs = get_nprocs();
            ftruncate(fd, sizeof(uint64_t) * nprocs * (max_item + 2));
            shared_cpu_cache_ = (uint64_t*)mmap(NULL, sizeof(uint64_t) * nprocs * (max_item + 2), port_flag, mm_flag, fd, 0);
            // ring buffer read and write pointer
            read_p_ = (uint64_t*)shared_cpu_cache_;
            write_p_ = (uint64_t*)shared_cpu_cache_ + nprocs;
            // init all cpu ring buffer with -1
            for (int i = 0; i < nprocs; i++) {
                read_p_[i] = 0;
                write_p_[i] = 0;
                for(int j = 0; j < max_item; j++) {
                    shared_cpu_cache_[2 * nprocs + i * max_item + j] = -1;
                }
            }
        }
        else {
            // open shm success, then mmap this file to local memory, and get ring buffer read and write
            int mm_flag   = MAP_SHARED | MAP_FIXED; 
            shared_cpu_cache_ = (uint64_t*)mmap(NULL, sizeof(uint64_t) * nprocs * (max_item + 2), port_flag, mm_flag, fd, 0);
            read_p_ = (uint64_t*)shared_cpu_cache_;
            write_p_ = (uint64_t*)shared_cpu_cache_ + nprocs;
        }
    }

    ~cpu_cache(){
        if(shared_cpu_cache_)
            munmap(shared_cpu_cache_, sizeof(uint64_t) * nprocs * (max_item + 2));
    }

    uint64_t get_cache_size(){
        // cache size maybe different, but static
        return cache_size_;
    }

    void free_cache(){
        // only the global host side need call this, to free all cpu_cache 
        if(shared_cpu_cache_)
            munmap(shared_cpu_cache_, sizeof(uint64_t) * nprocs * (max_item + 2));
        shm_unlink("/cpu_cache");
    }

    uint64_t fetch_cache(uint32_t nproc){
        // just fetch one block in the current cpu_id --> ring buffer
        uint64_t fetch_one = shared_cpu_cache_[2 * nprocs + nproc * max_item + read_p_[nproc]];
        if(fetch_one != -1) {
            shared_cpu_cache_[2 * nprocs + nproc * max_item + read_p_[nproc]] = -1;
            read_p_[nproc] = (read_p_[nproc] + 1) % max_item;
            return fetch_one;
        }
        else{
            return 0;
        }
    }

    void return_cache(uint32_t nproc, uint64_t addr){
        // using by local side, use to back a block at read_pointer as a fast return path
        uint32_t prev = (read_p_[nproc] - 1) % max_item;
        if(shared_cpu_cache_[2 * nprocs + nproc * max_item + prev] == -1){
            shared_cpu_cache_[2 * nprocs + nproc * max_item + prev] = addr;
            read_p_[nproc] = prev;
        }
    }

    void add_cache(uint32_t nproc, uint64_t addr){
        // host side fill cache, add write pointer
        if(shared_cpu_cache_[2 * nprocs + nproc * max_item + write_p_[nproc]] == -1){
            shared_cpu_cache_[2 * nprocs + nproc * max_item + write_p_[nproc]] = addr;
            write_p_[nproc] = (write_p_[nproc] + 1) % max_item;
        }
    }

    bool is_full(uint32_t nproc){
        return (write_p_[nproc] == read_p_[nproc] && write_p_[nproc] != -1);
    }

    bool is_empty(uint32_t nproc){
        return (write_p_[nproc] == read_p_[nproc] && write_p_[nproc] == -1);
    }

private:
    // uint64_t *base_addr_;
    uint64_t *shared_cpu_cache_;
    uint64_t *read_p_;
    uint64_t *write_p_;
    uint64_t cache_size_;
};

}