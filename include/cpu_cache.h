

#include <bits/stdint-uintn.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/sem.h>
#include <semaphore.h>

#pragma once

namespace mralloc {

// TODO: Add rseq support
// TODO: change to fifo pipe?
// TODO:

const uint32_t nprocs = 16;
const uint32_t max_alloc_item = 16;
const uint32_t max_free_item = 16;

enum LegoOpcode {LegoIdle, LegoReg, LegoAlloc, LegoFree, LegoDereg, LegoRemoteFree, LegoTransfer};

struct CpuBuffer {
    LegoOpcode opcode_;
    char buffer_[1024*1024];
    uint64_t doorbell_id;
    uint64_t retbell_id;
};

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
        volatile uint32_t reader = *reader_;
        volatile uint32_t writer = *writer_;
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
        volatile uint32_t writer = *writer_;
	    while(get_length() >= max_length_ - 1){
		    printf("busy wait\n");
	    }
        if(get_length() < max_length_-1){
            buffer_[writer] = value;        
            *writer_ = (writer + 1) % max_length_;
        }
    }

    void add_batch(T* value, uint64_t num) {
        volatile uint32_t writer = *writer_;
        if(get_length() < max_length_-num){
            for(int i = 0; i < num; i++){
                buffer_[(writer + i) % max_length_] = value[i];        
            }
            *writer_ = (writer + num) % max_length_;
        }
    }

    bool force_fetch_cache(T &value) {
        volatile uint32_t reader = *reader_;
        while(get_length() == 0 ) ;
        do{
            while(!(buffer_[reader] != zero_)){
                reader = *reader_;
            }
            *reader_ = (reader + 1) % max_length_;
        }while(!(buffer_[reader] != zero_));
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

    bool try_fetch_cache(T &value) {
        volatile uint32_t reader = *reader_;
        if(get_length() == 0) {
            return false;
        } 
        do{
            while(!(buffer_[reader] != zero_)){
                reader = *reader_;
            }
            *reader_ = (reader + 1) % max_length_;
        }while(!(buffer_[reader] != zero_));
        value = buffer_[reader];
        buffer_[reader] = zero_;
        return true;
    }

    bool try_fetch_batch(T* value, uint64_t num) {
        volatile uint64_t length = get_length();
        volatile uint32_t reader = *reader_;
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
        volatile uint64_t length = get_length();
        volatile uint32_t reader = *reader_;
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
    T* volatile buffer_;
    T zero_;
    volatile uint32_t* reader_;
    volatile uint32_t* writer_;
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
    T* volatile buffer_;
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
    mr_rdma_addr() {addr = -1; rkey = -1; node = -1;}
    mr_rdma_addr(uint64_t addr, uint32_t rkey, uint32_t node): addr(addr), rkey(rkey), node(node) {}
    bool operator==(mr_rdma_addr &compare) {
        return addr == compare.addr && rkey == compare.rkey && node == compare.node;
    }
    bool operator!=(mr_rdma_addr &compare) {
        return addr != compare.addr && rkey != compare.rkey && node != compare.node;
    }
    mr_rdma_addr& operator=(mr_rdma_addr &value) {
        this->addr = value.addr;
        this->rkey = value.rkey;
        this->node = value.node;
        return *this;
    }
    uint64_t addr;
    uint32_t rkey;
    uint32_t node;
} ;

class cpu_cache{
public:
    
    CpuBuffer* buffer_;

    sem_t* doorbell[nprocs]; 
    sem_t* retbell[nprocs];

    cpu_cache() {
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if (fd == -1) {
            perror("init failed, no computing node running");
        } else {
            int port_flag = PROT_READ | PROT_WRITE;
            int mm_flag   = MAP_SHARED; 
            buffer_ = (CpuBuffer*)mmap(NULL, sizeof(CpuBuffer)*nprocs, port_flag, mm_flag, fd, 0);   
            for(int i = 0; i < nprocs; i++) {
                doorbell[i] = sem_open(std::to_string(buffer_[i].doorbell_id).c_str(), O_CREAT, 0666, 0);
                retbell[i] = sem_open(std::to_string(buffer_[i].retbell_id).c_str(), O_CREAT, 0666, 0);
            }
        }
        
    }

    cpu_cache(uint64_t cache_size) {
        int port_flag = PROT_READ | PROT_WRITE;
        int mm_flag   = MAP_SHARED; 
        int fd = shm_open("/cpu_cache", O_RDWR, 0);
        if(fd==-1){
            fd = shm_open("/cpu_cache", O_CREAT | O_EXCL | O_RDWR, 0600);
            if(ftruncate(fd, sizeof(CpuBuffer)*nprocs)){
                perror("create shared memory failed");
            }
            buffer_ = (CpuBuffer*)mmap(NULL, sizeof(CpuBuffer)*nprocs, port_flag, mm_flag, fd, 0);
            for(int i = 0; i < nprocs; i++) {
                buffer_[i].doorbell_id = i+114514;
                buffer_[i].retbell_id = i+nprocs+114514;
                buffer_[i].opcode_ = LegoOpcode::LegoIdle;
                memset(buffer_[i].buffer_, 0, 1024*1024);
                doorbell[i] = sem_open(std::to_string(buffer_[i].doorbell_id).c_str(), O_CREAT, 0666, 0);
                retbell[i] = sem_open(std::to_string(buffer_[i].retbell_id).c_str(), O_CREAT, 0666, 0);
            }
        }
        else {
            buffer_ = (CpuBuffer*)mmap(NULL, sizeof(CpuBuffer)*nprocs, port_flag, mm_flag, fd, 0);
            for(int i = 0; i < nprocs; i++) {
                doorbell[i] = sem_open(std::to_string(buffer_[i].doorbell_id).c_str(), O_CREAT, 0666, 0);
                retbell[i] = sem_open(std::to_string(buffer_[i].retbell_id).c_str(), O_CREAT, 0666, 0);
            }
        }
    }

    ~cpu_cache(){
        if(buffer_)
            munmap(buffer_, sizeof(CpuBuffer)*nprocs);
    }

    void free_cache(){
        // only the global host side need call this, to free all cpu_cache 
        if(buffer_)
            munmap(buffer_,  sizeof(CpuBuffer)*nprocs);
        shm_unlink("/cpu_cache");
        for(int i=0; i<nprocs; i++){
            sem_close(doorbell[i]);
            sem_close(retbell[i]);
        }
    }

    bool malloc(mr_rdma_addr &addr){
        unsigned nproc;
        if((nproc = sched_getcpu()) == -1){
            printf("sched_getcpu bad \n");
            return false;
        }
        buffer_[nproc].opcode_ = LegoOpcode::LegoAlloc;
        sem_post(doorbell[nproc]);
        // printf("send request to %d\n", nproc);
        sem_wait(retbell[nproc]);
        // printf("recieve from %d\n", nproc);
        addr = *(mr_rdma_addr*)buffer_[nproc].buffer_;
        return true;
    }
    
    bool free(mr_rdma_addr addr){
        unsigned nproc;
        if((nproc = sched_getcpu()) == -1){
            printf("sched_getcpu bad \n");
            return false;
        }
        buffer_[nproc].opcode_ = LegoOpcode::LegoFree;
        *(mr_rdma_addr*)buffer_[nproc].buffer_ = addr;
        sem_post(doorbell[nproc]);
        // printf("send request to %d\n", nproc);
        sem_wait(retbell[nproc]);
        // printf("recieve from %d\n", nproc);
        return true;
    }
};

}
