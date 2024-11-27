
#include <bits/stdint-uintn.h>
#include <pthread.h>
#include <sys/types.h>
#include "cpu_cache.h"
#include "free_block_manager.h"
#include "computing_node.h"
#include "rdma_conn.h"
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <string>
#include <thread>

namespace mralloc {

void* run_rpc_thread(void* arg){
    ComputingNode* heap = (ComputingNode*)arg;
    heap->listenser();
    return NULL;
}

void ComputingNode::listenser() {
    section_e section_;
    uint32_t section_index_;
    region_e region_;
    uint32_t region_index_;
    uint32_t node_ = current_node_;
    std::vector<ConnectionManager*> m_rdma_conn;
    for(int i = 0; i < node_num_; i++){
        m_rdma_conn.push_back(new ConnectionManager());
        if (m_rdma_conn[i] == nullptr) return;
        if (m_rdma_conn[i]->init(ips[i], ports[i], 1, 1, node_id_)) return;
        sleep(1);
    }
    while(1) {
        MsgBuffer msg_buffer_;
        void* full_content;
        MRType type;
        for(int i = 0; i < node_num_; i++){
            m_rdma_conn[i]->remote_read((void*)&msg_buffer_, sizeof(MsgBuffer), 
                (uint64_t)node_info_[i].public_info_ + 1024*sizeof(uint64_t) + 1024*sizeof(uint16_t) + node_id_*sizeof(MsgBuffer), global_rkey_[i]);
            for(int j = 0; j < 8; j++){
                type = msg_buffer_.msg_type[j];
                if(type == MRType::MR_FREE) {
                    mr_rdma_addr free_addr;
                    m_rdma_conn[i]->remote_read((void*)&free_addr, sizeof(mr_rdma_addr),
                        (uint64_t)msg_buffer_.buffer, 
                        global_rkey_[i]);
                    assert(free_addr.addr != 0 && free_addr.rkey != 0);
                    mr_rdma_addr addr_empty;
                    memset((void*)&addr_empty, 0, sizeof(mr_rdma_addr));
                    
                    //set buffer to empty
                    m_rdma_conn[i]->remote_write((void*)&addr_empty, sizeof(mr_rdma_addr),
                        (uint64_t)msg_buffer_.buffer, 
                        global_rkey_[i]);

                    //set opcode to empty
                    MRType newtype = MRType::MR_IDLE;
                    m_rdma_conn[i]->remote_write((uint64_t*)&newtype, sizeof(MRType), 
                            (uint64_t)node_info_[free_addr.node].public_info_ + 1024*sizeof(uint64_t) + 1024*sizeof(uint16_t) + node_id_*sizeof(MsgBuffer) + sizeof(MRType)*j,
                            global_rkey_[free_addr.node]);    
                    

                    //TODO: update the free info to a page bitmap
                    mr_rdma_addr block_addr = free_addr;
                    uint64_t block_offset = block_addr.addr % node_info_[free_addr.node].block_size_;
                    block_addr.addr -= block_offset;
                    bitmap_record record = offset_record_[block_addr];
                    uint64_t bitmap_index = block_offset / record.bin_size /64;
                    uint64_t bitmap_offset = block_offset / record.bin_size %64;
                    uint64_t bitmap = cpu_cache_->bitmap_[record.offset + bitmap_index].load();
                    uint64_t new_bitmap= bitmap & ((uint64_t)1<<bitmap_offset);
                    do{
                        new_bitmap= bitmap & ((uint64_t)1<<bitmap_offset);
                    }while(!cpu_cache_->bitmap_[record.offset + bitmap_index].compare_exchange_weak(bitmap,new_bitmap));

                } else if(type == MRType::MR_TRANSFER) {
                    //TODO: read a segment and add it to local heap

                }
            }
        }
    }
    return;
}


void* run_woker_thread(void* arg){
    worker_param* param = (worker_param*)arg;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(param->id, &cpuset);
    pthread_t this_tid = pthread_self();
    uint64_t ret = pthread_setaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    ret = pthread_getaffinity_np(this_tid, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i ++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("filler running on core: %d\n" , i);
        }
    }
    param->heap->woker(param->id);
    return NULL;
}

void ComputingNode::woker(int proc) {
    section_e section_;
    uint32_t section_index_;
    region_e region_;
    uint32_t region_index_;
    uint32_t node_ = current_node_;
    std::vector<ConnectionManager*> m_rdma_conn;
    for(int i = 0; i < node_num_; i++){
        m_rdma_conn.push_back(new ConnectionManager());
        if (m_rdma_conn[i] == nullptr) return;
        if (m_rdma_conn[i]->init(ips[i], ports[i], 1, 1, node_id_)) return;
        sleep(1);
    }
    m_rdma_conn[node_]->find_section(section_, section_index_, mralloc::alloc_light);
    m_rdma_conn[node_]->fetch_region(section_, section_index_, 
                            true, false, region_, region_index_, 0);
    while(1){
        sem_wait(cpu_cache_->doorbell[proc]);
        // printf("recieve request from %d\n", proc);
        switch(cpu_cache_->buffer_[proc].opcode_){
            
            case LegoOpcode::LegoAlloc: {
                uint16_t bin_size = *(uint64_t*)cpu_cache_->buffer_[proc].buffer_;
                // if(bin_size != 0){
                // //TODO: check bin_size, first find if there are segment avaliable
                // // find_segment(bin_size);
                // }
                int retry_time = 0, cas_time = 0, section_time = 0, region_time = 0, result;
                bool slow_path = false;
                mr_rdma_addr remote_addr;
                remote_addr.node = node_;
                while((result = m_rdma_conn[node_]->fetch_region_block(section_, region_, remote_addr.addr, remote_addr.rkey, false, region_index_, bin_size)) < 0){
                    cas_time += (-1)*result;
                    if(!slow_path){
                        while((result = m_rdma_conn[node_]->fetch_region(section_, section_index_, true, false, region_, region_index_, 0)) < 0){
                            region_time += (-1)*result;
                            if((result = m_rdma_conn[node_]->find_section(section_, section_index_, mralloc::alloc_light)) < 0){
                                slow_path = true;
                                section_time += (-1)*result;
                                break;
                            }else section_time += result;
                        }
                        region_time += result;
                    } else {
                        while((result = m_rdma_conn[node_]->fetch_region(section_, section_index_, true, true, region_, region_index_, 0)) < 0){
                            region_time += (-1)*result;
                            if((result = m_rdma_conn[node_]->find_section(section_, section_index_, mralloc::alloc_heavy)) < 0){
                                section_time += (-1)*result;
                                node_ = (node_+1) % node_num_;
                                remote_addr.node = node_;
                                printf("[cache single region] try to scan next node %d\n", node_);
                            }
                            else section_time += result;
                        }  
                        region_time += result;
                    }
                }
                cas_time += result;
                if(cas_time > retry_threshold && !slow_path) {
                    if((result = m_rdma_conn[node_]->find_section(section_, section_index_, mralloc::alloc_light)) < 0){
                        slow_path = true;
                        section_time += (-1)*result;
                    }else section_time += result;
                    while((result = m_rdma_conn[node_]->fetch_region(section_, section_index_, true, false, region_, region_index_, 0)) < 0){
                        region_time += (-1)*result;
                        if((result = m_rdma_conn[node_]->find_section(section_, section_index_, mralloc::alloc_light)) < 0){
                            slow_path = true;
                            section_time += (-1)*result;
                            break;
                        }else section_time += result;
                    }
                }
                retry_time = cas_time + section_time + region_time;
                remote_addr.size = bin_size;
                *(mr_rdma_addr*)cpu_cache_->buffer_[proc].buffer_ = remote_addr;
                // if(bin_size != 0){
                //     uint64_t shm_offset = cpu_cache_->bitmap_malloc(bin_size);
                //     *(uint64_t*)((mr_rdma_addr*)cpu_cache_->buffer_[proc].buffer_+1) = shm_offset;
                //     bitmap_record record = {shm_offset, bin_size};
                //     //offset_record_[remote_addr] = record;
                // }
                cpu_cache_->buffer_[proc].opcode_ = LegoOpcode::LegoIdle;
                break;
            }
            
            case LegoOpcode::LegoFree: {
                mr_rdma_addr remote_addr = *(mr_rdma_addr*)cpu_cache_->buffer_[proc].buffer_;
                m_rdma_conn[remote_addr.node]->free_region_block(remote_addr.addr, false, remote_addr.size);
                cpu_cache_->buffer_[proc].opcode_ = LegoOpcode::LegoIdle;
                break;
            }

            case LegoOpcode::LegoReg: {
                uint16_t process_id = *(uint16_t*)cpu_cache_->buffer_[proc].buffer_;
                for(int i = 0; i < node_num_; i++){
                    uint64_t alive_count;
                    m_rdma_conn[i]->remote_read(&alive_count, sizeof(uint64_t), (uint64_t)node_info_[i].public_info_ + sizeof(uint64_t)*process_id, global_rkey_[i]);
                    while(m_rdma_conn[i]->remote_CAS(alive_count+1, &alive_count, (uint64_t)node_info_[i].public_info_ + sizeof(uint64_t)*process_id, global_rkey_[i]));
                }
                cpu_cache_->buffer_[proc].opcode_ = LegoOpcode::LegoIdle;
                break;
            }

            case LegoOpcode::LegoDereg: {
                uint16_t process_id = *(uint16_t*)cpu_cache_->buffer_[proc].buffer_;
                for(int i = 0; i < node_num_; i++){
                    uint64_t alive_count;
                    m_rdma_conn[i]->remote_read(&alive_count, sizeof(uint64_t), (uint64_t)node_info_[i].public_info_ + sizeof(uint64_t)*process_id, global_rkey_[i]);
                    while(m_rdma_conn[i]->remote_CAS(alive_count-1, &alive_count, (uint64_t)node_info_[i].public_info_ + sizeof(uint64_t)*process_id, global_rkey_[i]));
                }
                cpu_cache_->buffer_[proc].opcode_ = LegoOpcode::LegoIdle;
                break;
            }

            case LegoOpcode::LegoRemoteFree: {

                mr_rdma_addr free_addr;
                block_e owner_block_header;
                uint64_t block_index = (free_addr.addr-node_info_[free_addr.node].heap_start_)/node_info_[free_addr.node].block_size_;
                
                // read block's owner id 
                m_rdma_conn[free_addr.node]->remote_read((void*)&owner_block_header, sizeof(block_e), 
                    node_info_[free_addr.node].block_header_ + sizeof(uint64_t)*block_index, global_rkey_[free_addr.node]);
                if(owner_block_header.client_id_ == 0){
                    printf("free an invaliadate block!\n");
                }
                uint16_t owner_id = owner_block_header.client_id_ - 1;
                uint16_t node_id;

                // read owner's node id
                m_rdma_conn[free_addr.node]->remote_read((void*)&node_id, sizeof(uint16_t), 
                    (uint64_t)node_info_[free_addr.node].public_info_ + sizeof(uint64_t) * 1024 + sizeof(uint16_t) * owner_id, 
                    global_rkey_[free_addr.node]);
                MsgBuffer msg_buffer_;
                
                // read current msg buffer
                m_rdma_conn[free_addr.node]->remote_read((void*)&msg_buffer_, sizeof(MsgBuffer), 
                    (uint64_t)node_info_[free_addr.node].public_info_ + 1024*sizeof(uint64_t) + 1024*sizeof(uint16_t) + node_id*sizeof(MsgBuffer), 
                    global_rkey_[free_addr.node]);
                
                // send msg to node's heap
                for(int i = 0; i < 8; i++){
                    if(msg_buffer_.msg_type[i]==MRType::MR_IDLE){

                        // set opcode
                        while(m_rdma_conn[free_addr.node]->remote_CAS(MRType::MR_FREE, (uint64_t*)&msg_buffer_.msg_type[i], 
                            (uint64_t)node_info_[free_addr.node].public_info_ + 1024*sizeof(uint64_t) + 1024*sizeof(uint16_t) + node_id*sizeof(MsgBuffer) + sizeof(MRType)*i,
                            global_rkey_[free_addr.node]));
                        
                        // set memory
                        m_rdma_conn[free_addr.node]->remote_write((void*)&free_addr, sizeof(mr_rdma_addr),
                            (uint64_t)msg_buffer_.buffer, 
                            global_rkey_[free_addr.node]);
                        
                        break;
                    }
                }                
                cpu_cache_->buffer_[proc].opcode_ = LegoOpcode::LegoIdle;
                break;
            }
            case LegoOpcode::LegoTransfer: {
                //TODO: Write a full segment with pages and bitmaps
                break;
            }
            default :{
                printf("Wrong opcode\n");
                break;
            }
        }
        sem_post(cpu_cache_->retbell[proc]);
    }
    return;
}

/**
  * @description: start local memory service
  * @param {string} addr   the address string of RemoteHeap to connect
  * @param {string} port   the port of RemoteHeap to connect
  * @return {bool} true for success
  */
bool ComputingNode::start(std::string* addr, std::string* port, uint32_t node_num){
    use_global_rkey_ = true;
    node_num_ = node_num;
    for(int i = 0; i < node_num; i++){
        m_rdma_conn_.push_back(new ConnectionManager());
        ips.push_back(std::string(addr[i])); ports.push_back(std::string(port[i]));
        if (m_rdma_conn_[i] == nullptr) return -1;
        if (m_rdma_conn_[i]->init(addr[i], port[i], 1, 1, node_id_)) return false;
        sleep(1);
        set_global_rkey(m_rdma_conn_[i]->get_global_rkey(), i);
    }
    // init free queue manager, using REMOTE_BLOCKSIZE as init size
    running = 1;
    if(heap_enabled_ && one_side_enabled_) {
        bool ret;
        for(int i = 0; i < node_num; i++){
            one_side_info m_one_side_info_ = m_rdma_conn_[i]->get_one_side_info();
            node_info new_info(m_one_side_info_);
            node_info_.push_back(new_info);
            exclusive_region_.push_back((region_with_rkey*)malloc(sizeof(region_with_rkey) * new_info.region_num_));
        }
        hint_ = rand()%node_info_[0].block_num_;
        // ret = new_cache_section(alloc_empty, rand()%node_num_);
        // ret &= new_backup_section(rand()%node_num_);
        // ret &= new_cache_region();
        // ret &= new_backup_region();
        // cache_watermark_high = 0.99;
        // cache_watermark_low = 0.01;
        // cache_upper_bound = 145;
        // ret &= fill_cache_block();
        // if(!ret) {
        //     printf("init cache failed\n");
        //     return false;
        // }
    } 
    // else if(one_side_enabled_) {
    //     new_cache_section(alloc_empty, rand()%node_num_);
    //     new_cache_region();
    // } else if(heap_enabled_) {
    //     printf("RPC not implemented now\n");
    // }
    // init cpu cache, insert a block for each cpu cache ring buffer
    if(cpu_cache_enabled_) {
        mr_rdma_addr remote_addr;
        cpu_cache_ = new cpu_cache(node_info_[0].block_size_);
        // for(int i = 0; i < nprocs; i++){
        //     fetch_mem_block(remote_addr);
        //     assert(remote_addr.addr!=0);
        //     cpu_cache_->add_cache(i, remote_addr);
        //     printf("init @%d addr:%lx rkey:%u\n", i, remote_addr.addr, remote_addr.rkey);
        // }
        pthread_t running_thread;
        pthread_t listening_thread;
        // pthread_create(&cache_fill_thread_, NULL, run_cache_filler, this);
        // pthread_create(&recycle_thread_, NULL, run_recycler, this);   
        worker_param new_param[nprocs];
        for(int i = 0; i < nprocs; i++) {
            new_param[i].heap = this; new_param[i].id = i;
            pthread_create(&woker_thread_[i], NULL, run_woker_thread, &new_param[i]);
            // sleep(1);
            usleep(100000);
        }
        //pthread_create(&listening_thread, NULL, run_rpc_thread, this);
    }
    return true;
}

void ComputingNode::increase_watermark(int &upper_bound) {
    if (upper_bound == ring_buffer_size - 16 || upper_bound >= 4 * block_per_region) {
        return;
    }
    if(upper_bound < block_per_region) {
        upper_bound *= 2;
    }
    else if(upper_bound >= 8*block_per_region) {
        upper_bound += block_per_region;
    } else {
        upper_bound += 8;
    }
}

void ComputingNode::decrease_watermark(int &upper_bound) {
    if(upper_bound > 2*block_per_region) {
        upper_bound -= block_per_region;
    } else if (upper_bound > block_per_region){
        upper_bound -= 8;
    }
}
// a infinite loop worker
void ComputingNode::pre_fetcher() {
    std::unordered_map<uint32_t, uint32_t> region_map; 
    uint64_t addr_offset=0;
    uint32_t his_length=0;
    uint32_t idle_cycle = 0;
    volatile uint64_t update_time = 0;
    uint32_t length=0;
    int recycle_counter = 0; uint64_t recycle_addr[32]; uint32_t new_rkey[32];
    uint64_t addr=0, batch_addr[max_free_item];
    bool block_breakdown = false;
    cache_upper_bound = block_per_region;
    fill_cache_block();
    while(running) {
	int free_num;
	int total_free = 0;
        if(ring_cache->get_length() == 0) {
            block_breakdown = true;
            increase_watermark(cache_upper_bound);
            fill_cache_block();
        }
        length = ring_cache->get_length();
        if(update_time != time_stamp_) {
            update_time = time_stamp_;
            // fill the block cache
            if(block_breakdown) {
                block_breakdown = false;
            } else {
                if(length <= cache_upper_bound * cache_watermark_low || length <= 8) {
                    increase_watermark(cache_upper_bound);
                    fill_cache_block();
                } else if(length < cache_upper_bound && length >= cache_upper_bound *cache_watermark_high){
                    decrease_watermark(cache_upper_bound);
                } else if(length < cache_upper_bound ) {
                    decrease_watermark(cache_upper_bound);
			        fill_cache_block();
                } 
            }
        }
	    length = ring_cache->get_length();
        if(length > cache_upper_bound ) {
            if(his_length != 0 && his_length == length &&length - cache_upper_bound > block_per_region){
			idle_cycle += 1;
		    if(idle_cycle > 10000){		
	    	    mr_rdma_addr addr; 
				for(int k = 0; k < block_per_region; k++) {
					if(ring_cache->try_fetch_cache(addr))
                        free_mem_block_slow(addr);
				}
				his_length = length - block_per_region;
		} else {his_length = length;}
			} 
	    else {his_length = length;idle_cycle = 0;}		
        } else {his_length = 0;idle_cycle = 0;}
    }
}

// void ComputingNode::recycler() {
//     uint64_t addr, length;
//     mr_rdma_addr batch_addr[max_free_item];
//     int recycle_counter = 0; uint64_t recycle_addr[32]; uint32_t new_rkey[32];
//     while(running) {
//         int free_num;
// 	    int total_free = 0;
// 	    do{
// 	        free_num = 0; 
//             for(int i = 0; i < nprocs; i++){
//                 if((length = cpu_cache_->fetch_free_cache(i, batch_addr)) != 0) {
//                     free_num += length;
//                     for(int j = 0; j < length; j++){
//                         uint64_t region_offset = (batch_addr[j].addr - node_info_[batch_addr[j].node].heap_start_) / node_info_[batch_addr[j].node].region_size_;
//                         uint64_t region_block_offset = (batch_addr[j].addr - node_info_[batch_addr[j].node].heap_start_) 
//                             % node_info_[batch_addr[j].node].region_size_ / node_info_[batch_addr[j].node].block_size_;
//                         uint32_t new_key = m_rdma_conn_[batch_addr[j].node]->rebind_region_block_rkey(region_offset, region_block_offset);
//                         if(new_key != 0) {
//                             mr_rdma_addr result; 
//                             result = batch_addr[j]; 
//                             exclusive_region_[batch_addr[j].node][region_offset].rkey[region_block_offset] = new_key;
//                             result.rkey = new_key;
//                             ring_cache->add_cache(result);
//                         } else {
//                             printf("Do not use batch for rebind anymore\n");
//                             // if(recycle_counter > 31) {
//                             //     recycle_counter = 0;
//                             //     free_mem_block_fast_batch(recycle_addr);
//                             // }
//                             // recycle_addr[recycle_counter] = batch_addr[j];
//                             // recycle_counter ++;
//                         }
//                     }
//                 }
//             }
// 	        total_free += free_num;
//         }while(free_num >= 8);
//     }
// }

// void ComputingNode::print_cpu_cache() {
//     for(int i = 0;i < nprocs; i++) {
//         if(cpu_cache_watermark[i] != 1 || cpu_cache_->get_length(i) != 1)
//             printf("%d:%d/%u\t", i, cpu_cache_->get_length(i), cpu_cache_watermark[i]);
//     }
//     printf("\n");

// }

// // a infinite loop worker
// void ComputingNode::cache_filler() {
//   // scan the cpu cache and refill them
//     time_stamp_ = 0; uint64_t update = 0;

//     for(int i=0; i<nprocs; i++) {
//         cpu_cache_watermark[i] = 1;
//     }
//     uint64_t init_addr_ = -1; uint32_t init_rkey_ = -1;
//     mr_rdma_addr batch_addr[64];
//     while(running) {
//         update = 0;
//         for(int i = 0; i < nprocs; i++){
//             int free_ = cpu_cache_->get_length(i);
//             // empty    --> fill and +1
//             // 1 left   --> fill
//             // > 1 left --> fill and -1
//             if(free_ == 0){
//                 if (cpu_cache_watermark[i] < 8)
//                     cpu_cache_watermark[i] += 1;
//                 while(!ring_cache->try_fetch_batch(batch_addr, cpu_cache_watermark[i])){
//                     time_stamp_ += 1;
//                 }
//                 cpu_cache_->add_batch(i, batch_addr, cpu_cache_watermark[i]);
//                 update += cpu_cache_watermark[i];
//             }
//             else if(free_ < cpu_cache_watermark[i] && free_ > 1) {
//                 if(cpu_cache_watermark[1] > 1)
//                     cpu_cache_watermark[i] -= 1;
//                 while(!ring_cache->try_fetch_batch(batch_addr, cpu_cache_watermark[i]-free_)){
//                     time_stamp_ += 1;
//                 }
//                 cpu_cache_->add_batch(i, batch_addr, cpu_cache_watermark[i]-free_);
//                 update += cpu_cache_watermark[i] - free_;
//             } else if(cpu_cache_watermark[i] > 1 && free_ == 1) {
//                 while(!ring_cache->try_fetch_batch(batch_addr, cpu_cache_watermark[i]-free_)){
//                     time_stamp_ += 1;
// 		}
//                 cpu_cache_->add_batch(i, batch_addr, cpu_cache_watermark[i]-free_);
//                 update += cpu_cache_watermark[i] - free_;
//             } 
//         }
//         if(update) {
//             time_stamp_ += 1;
//         }
//     }
// }

bool ComputingNode::new_cache_section(alloc_advise advise, uint32_t node){
    if(!m_rdma_conn_[node]->find_section(current_section_, current_section_index_, advise) ) {
        printf("cannot find avaliable section\n");
        return false;
    }
    current_node_ = node;
    return true;
}

bool ComputingNode::new_backup_section(uint32_t node){
    if(!m_rdma_conn_[node]->find_section(backup_section_, backup_section_index_, alloc_light) ) {
        printf("cannot find avaliable backup section\n");
        return false;
    }
    backup_node_ = node;
    return true;
}

bool ComputingNode::new_cache_region() {
    // exclusive, and fetch rkey must
    region_e new_region; uint32_t new_region_index;
    while(!m_rdma_conn_[current_node_]->fetch_region(current_section_, current_section_index_, false, false, new_region, new_region_index, 0) ) {
        if(!new_cache_section(alloc_empty, current_node_)){
            current_node_ = (current_node_+1) % node_num_;
            printf("[cache single region] try to scan next node %d\n", current_node_);
        }
    }
    current_region_ = &exclusive_region_[current_node_][new_region_index];
    current_region_->region = new_region;
    current_region_->index = new_region_index;
    m_rdma_conn_[current_node_]->fetch_exclusive_region_rkey(current_region_->index, current_region_->rkey);
    return true;
}

bool ComputingNode::new_backup_region() {
        // exclusive, and fetch rkey must
    while(!m_rdma_conn_[backup_node_]->fetch_region(backup_section_, backup_section_index_, true, true, backup_region_, backup_region_index_, 0) ) {
        if(!new_backup_section(backup_node_)){
            backup_node_ = (backup_node_+1) % node_num_;
            printf("[cache backup region] try to scan next node %d\n", backup_node_);
            // return false;
        }
    }
    backup_counter += 1;
    exclusive_region_[backup_node_][backup_region_index_].region = backup_region_;
    exclusive_region_[backup_node_][backup_region_index_].index = backup_region_index_;
    return true;
}

int block_counter = 0;

bool ComputingNode::fill_cache_block(){
    int length =  ring_cache->get_length();
	if(cache_upper_bound > length) {
		block_counter += (cache_upper_bound - length);
	}
    mr_rdma_addr addr;
    for(int i = 0; i< cache_upper_bound - length; i++){
        if(current_region_->region.base_map_ != bitmap32_filled) {
            int index = find_free_index_from_bitmap32_tail(current_region_->region.base_map_);
            current_region_->region.base_map_ |= 1<<index;
            mr_rdma_addr addr(get_region_block_addr(current_region_->index, index, current_region_->node), current_region_->rkey[index].main_rkey_, current_region_->node);
            ring_cache->add_cache(addr);
            fill_counter += 1;
        } else {
            m_mutex_.lock();
            if(!free_region_.empty()) {
                current_region_ = *free_region_.begin();
                free_region_.erase(free_region_.begin());
                m_mutex_.unlock();
                return fill_cache_block();
            }
            m_mutex_.unlock();
            if(backup_cycle >= 0 && backup_counter >= backup_cycle) {
                backup_counter = 0;
                if(new_cache_region()) {
                    return fill_cache_block();
                } 
            }
            int block_num = cache_upper_bound - ring_cache->get_length(), get_num;
            mr_rdma_addr addr[block_per_region];
            while(block_num > 0){
                while((get_num = m_rdma_conn_[backup_node_]->fetch_region_batch(backup_section_, backup_region_, addr, block_num, false, backup_region_index_)) == 0){
                    new_backup_region();
                }
                uint64_t offset = backup_region_index_;
                uint64_t region_start = node_info_[backup_node_].heap_start_ + offset * node_info_[backup_node_].region_size_;
                for(int j = 0; j < get_num; j++) {
                    exclusive_region_[backup_node_][offset].rkey[(addr[j].addr-region_start)/node_info_[backup_node_].block_size_].main_rkey_ = addr[j].rkey;
                    addr[j].node = backup_node_;
                }
                block_num -= get_num;
                fill_counter += get_num;
                ring_cache->add_batch(addr, get_num);
            };
            return true;
        }
    }
    return true;
}

bool ComputingNode::fetch_mem_block(mr_rdma_addr &remote_addr){
    // mr_rdma_addr result;
    bool ret = ring_cache->force_fetch_cache(remote_addr);
    // addr = result.addr; 
    // rkey = result.rkey;
    return ret;
}

bool ComputingNode::free_mem_batch(uint32_t region_offset, uint32_t free_map, uint32_t node){
    int free_length = free_bit_in_bitmap32(free_map), index;
    mr_rdma_addr result[block_per_region];
    region_with_rkey* region;
    if(exclusive_region_[node][region_offset].region.exclusive_ == 1 && ring_cache->get_length() + free_length < cache_upper_bound * cache_watermark_high) {
        for(int i = 0; i < free_length; i++) {
            index = find_free_index_from_bitmap32_tail(free_map);
            free_map |= (uint32_t)(1<<index);
            result[i].addr = get_region_block_addr(region_offset, index, node);
            result[i].rkey = exclusive_region_[node][region_offset].rkey[index].main_rkey_;
            result[i].node = node;
        } 
        ring_cache->add_batch(result, free_length);
        return true;
    } else if (exclusive_region_[node][region_offset].region.on_use_ == 1 && ring_cache->get_length()+ free_length < cache_upper_bound * cache_watermark_high){
        for(int i = 0; i < free_length; i++) {
            index = find_free_index_from_bitmap32_tail(free_map);
            free_map |= (uint32_t)(1<<index);
            result[i].addr = get_region_block_addr(region_offset, index, node);
            result[i].rkey = exclusive_region_[node][region_offset].rkey[index].main_rkey_;
            result[i].node = node;
        } 
        ring_cache->add_batch(result, free_length);
        return true;
    }
    if(region_offset == current_region_->index) {
        region = current_region_;  
        if(true){
            region->region.base_map_ &= free_map;
            return true;
        }  
        return false;
    } else if( exclusive_region_[node][region_offset].region.exclusive_ == 0 ){
        int free_class;
        if((free_class = m_rdma_conn_[node]->free_region_batch(region_offset, free_map, false)) == -1){
            printf("Free the remote metadata failed\n");
            return false;
        }
        if(region_offset == backup_region_index_) {
            backup_region_.base_map_ &= free_map;  
        }
        if(free_class == -2) free_class = 1;
        fill_counter -= (free_class+1);
        return true;
    } else {
        region = &exclusive_region_[node][region_offset];
        if(true){
            region->region.base_map_ &= free_map;
            if(free_bit_in_bitmap32(region->region.base_map_) == block_per_region) {
                m_mutex_.lock();
                auto result = free_region_.find(region);
                if (result != free_region_.end()){
                    free_region_.erase(result);
                }
                m_mutex_.unlock();
                // m_rdma_conn_[node]->set_region_empty(region->region, region_offset);
                m_rdma_conn_[node]->force_update_region_state(region->region, region_offset, false, true);
                section_e empty_section;
                m_rdma_conn_[node]->force_update_section_state(empty_section, region_offset, alloc_empty);
                region->region.exclusive_ = 0;
            }
            else if(free_bit_in_bitmap32(region->region.base_map_) < block_per_region/2){
                m_mutex_.lock();
                if(free_region_.find(region) == free_region_.end())
                    free_region_.insert(region);
                m_mutex_.unlock();
            }
            return true;
        }
    }
    return false;
}

bool ComputingNode::free_mem_block_slow(mr_rdma_addr remote_addr) {
    uint32_t node = remote_addr.node;
    uint64_t region_offset = (remote_addr.addr - node_info_[node].heap_start_) / node_info_[node].region_size_;
    uint64_t region_block_offset = (remote_addr.addr - node_info_[node].heap_start_) % node_info_[node].region_size_ / node_info_[node].block_size_;
    region_with_rkey* region;
    if(region_offset == current_region_->index) {
        region = current_region_;  
        if(true){
            region->region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
            return true;
        }  
        return false;
    }
    else if( exclusive_region_[node][region_offset].region.exclusive_ == 0 ){
        int free_class;
        if((free_class = m_rdma_conn_[node]->free_region_block(remote_addr.addr, false, 0)) == -1){
            printf("Free the remote metadata failed\n");
            return false;
        }
        // if(free_class == -2 && region_offset == backup_region_index_){
        //     m_rdma_conn_[node]->set_region_empty(backup_region_, region_offset);
        //     new_backup_region();
        // } else 
        if(region_offset == backup_region_index_) {
            backup_region_.base_map_ &= ~(uint32_t)(1<<region_block_offset);  
        } 
        // else if(free_class == -2) {
	    //     m_rdma_conn_[node]->set_region_empty(exclusive_region_[node][region_offset].region, region_offset);
	    // }
        if(free_class == -2) free_class = 1;
        fill_counter -= (free_class+1);
        return true;
    } else {
        region = &exclusive_region_[node][region_offset];
        if(true){
            region->region.base_map_ &= ~(uint32_t)(1<<region_block_offset);
            int free_length = free_bit_in_bitmap32(region->region.base_map_);
	    if(free_length == block_per_region) {
                m_mutex_.lock();
                auto result = free_region_.find(region);
                if (result != free_region_.end()){
                    free_region_.erase(result);
                }
                m_mutex_.unlock();
                // m_rdma_conn_[node]->set_region_empty(region->region, region_offset);
                m_rdma_conn_[node]->force_update_region_state(region->region, region_offset, false, true);
                section_e empty_section;
                m_rdma_conn_[node]->force_update_section_state(empty_section, region_offset, alloc_empty);
                region->region.exclusive_ = 0;
            }
            else if(free_length < block_per_region/10){
                m_mutex_.lock();
                if(free_region_.find(region) == free_region_.end())
                    free_region_.insert(region);
                m_mutex_.unlock();
            } else if (free_length > 9*block_per_region/10) {
                m_mutex_.lock();
                auto i = free_region_.find(region);
                if(i != free_region_.end()) {
                    free_region_.erase(i);
                }
                m_mutex_.unlock();
	        }
            return true;
        }
    }
    return false;
}

bool ComputingNode::free_mem_block(mr_rdma_addr remote_addr){
    uint64_t addr = remote_addr.addr; uint32_t node = remote_addr.node; uint32_t rkey = remote_addr.rkey;
    uint64_t region_offset = (addr - node_info_[node].heap_start_) / node_info_[node].region_size_;
    uint64_t region_block_offset = (addr - node_info_[node].heap_start_) % node_info_[node].region_size_ / node_info_[node].block_size_;
    region_with_rkey* region; 
    m_rdma_conn_[node]->remote_rebind(addr, exclusive_region_[node][region_offset].rkey[region_block_offset].main_rkey_);
    if(exclusive_region_[node][region_offset].region.exclusive_ == 1 && ring_cache->get_length() < 200*block_per_region) {
        mr_rdma_addr result; 
        result.addr = addr; 
        result.rkey = exclusive_region_[node][region_offset].rkey[region_block_offset].main_rkey_;
        ring_cache->add_cache(result);
        return true;
    } else if (exclusive_region_[node][region_offset].region.on_use_ == 1 && ring_cache->get_length() < 200*block_per_region){
        mr_rdma_addr result; 
        result.addr = addr; 
        result.rkey = exclusive_region_[node][region_offset].rkey[region_block_offset].main_rkey_;
        ring_cache->add_cache(result);
        return true;
    }
    return free_mem_block_slow(remote_addr);
}

// This is a DEBUG interface for a direct block fetch using LegoHeader
bool ComputingNode::fetch_mem_block_nocached(mr_rdma_addr &remote_addr, uint32_t node){
    while(!m_rdma_conn_[node]->fetch_region_block(current_section_ ,current_region_->region, remote_addr.addr, remote_addr.rkey, false, current_region_->index, 0)) {
        new_cache_region();
    }
    remote_addr.node = node;
    if(use_global_rkey_) remote_addr.rkey = get_global_rkey(node);
    return true;
}

// void ComputingNode::fetch_cache(uint8_t nproc, mr_rdma_addr &remote_addr) {
//   cpu_cache_->fetch_cache(nproc, remote_addr);
//   return;
// }

/**
  * @description: stop local memory service
  * @return {void}
  */
void ComputingNode::stop(){
  running = 0;
  if(cpu_cache_enabled_) cpu_cache_->free_cache();
};

/**
  * @description: get memory alive state
  * @return {bool}  true for alive
  */
bool ComputingNode::alive() { return true; }

// API for test
bool ComputingNode::fetch_mem_block_remote(mr_rdma_addr &remote_addr, uint32_t node) {
    if (m_rdma_conn_[node]->remote_fetch_block(remote_addr.addr, remote_addr.rkey)) return false;
    remote_addr.node = node;
    return true;
}
 
// API for test
bool ComputingNode::mr_bind_remote(uint64_t size, mr_rdma_addr remote_addr, uint32_t &newkey) {
  if (m_rdma_conn_[remote_addr.node]->remote_mw(remote_addr.addr, remote_addr.rkey, size, newkey)) return false;
  return true;
}

}
