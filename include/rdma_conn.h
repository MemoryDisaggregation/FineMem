/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-07-24 10:13:26
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-12-05 17:11:41
 * @FilePath: /rmalloc_newbase/include/rdma_conn.h
 * @Description: RDMA Connection functions, with RDMA read/write and fetch block, used by both LocalHeap and RemoteHeap
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */
#pragma once

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <infiniband/verbs.h>
#include <map>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <string>
#include "msg.h"
#include "free_block_manager.h"
#include "cpu_cache.h"

namespace mralloc {

#define RESOLVE_TIMEOUT_MS 5000

struct one_side_info {
    uint64_t block_size_;
    uint64_t block_num_;
    uint32_t global_rkey_;
    uint64_t section_header_;
    uint64_t heap_start_;
};

/* RDMA connection */
class RDMAConnection {
public:
    int init(const std::string ip, const std::string port, uint8_t access_type);
    one_side_info get_one_side_info() {return m_one_side_info_;};
    int register_remote_memory(uint64_t &addr, uint32_t &rkey, uint64_t size);
    int unregister_remote_memory(uint64_t addr);
    int remote_read(void *ptr, uint64_t size, uint64_t remote_addr,
                    uint32_t rkey);
    int remote_write(void *ptr, uint64_t size, uint64_t remote_addr,
                    uint32_t rkey);
    bool remote_CAS(uint64_t swap, uint64_t* compare, uint64_t remote_addr, 
                        uint32_t rkey);
    int remote_fetch_block(uint64_t &addr, uint32_t &rkey, uint64_t size);
    int remote_fetch_block(uint64_t &addr, uint32_t &rkey);
    int remote_free_block(uint64_t addr);
    int remote_mw(uint64_t addr, uint32_t rkey, uint64_t size, uint32_t &newkey);
    int remote_rebind(uint64_t addr, uint32_t block_class, uint32_t &newkey);
    int remote_rebind_batch(uint64_t *addr, uint32_t *newkey);
    int remote_class_bind(uint32_t region_offset, uint16_t block_class);
    int remote_memzero(uint64_t addr, uint64_t size);
    int remote_fusee_alloc(uint64_t &addr, uint32_t &rkey);
    int remote_print_alloc_info();
    uint32_t get_rkey() {return m_fusee_rkey;};
    uint32_t get_global_rkey() {return global_rkey_;};
    ibv_qp* get_qp() {return m_cm_id_->qp;};
    ibv_cq* get_cq() {return m_cq_;};
    ibv_pd* get_pd() {return m_pd_;};
    ibv_context* get_ctx() {return m_cm_id_->verbs;};

    // << one-sided fetch API >>

    inline uint64_t section_metadata_addr(uint64_t section_offset) {return (uint64_t)((section_e*)section_header_ + section_offset);};
    inline uint64_t section_class_metadata_addr(uint64_t section_class_offset) {return (uint64_t)((section_class_e*)section_class_header_ + section_class_offset);};
    inline uint64_t region_metadata_addr(uint64_t region_offset) {return (uint64_t)((region_e*)region_header_ + region_offset);};

    uint64_t get_heap_start() {return heap_start_;};
    inline bool check_section(section_e alloc_section, alloc_advise advise, uint32_t offset);
    bool update_section(uint32_t region_index, alloc_advise advise, alloc_advise compare);
    bool find_section(uint16_t block_class, section_e &alloc_section, uint32_t &section_offset, alloc_advise advise) ;

    bool fetch_large_region(section_e &alloc_section, uint32_t section_offset, uint64_t region_num, uint64_t &addr) ;
    bool fetch_region(section_e &alloc_section, uint32_t section_offset, uint32_t block_class, bool shared, region_e &alloc_region, uint32_t &region_index) ;
    bool try_add_section_class(uint32_t section_offset, uint32_t block_class, uint32_t region_index);
    bool set_region_exclusive(region_e &alloc_region, uint32_t region_index);
    bool set_region_empty(region_e &alloc_region, uint32_t region_index);
    bool fetch_exclusive_region_rkey(uint32_t region_index, uint32_t* rkey_list) {
        // uint32_t new_rkey[block_per_region];
        // memset(new_rkey, (uint32_t)-1, sizeof(uint32_t)*block_per_region);
        remote_read(rkey_list, sizeof(uint32_t)*block_per_region, block_rkey_ + region_index*block_per_region*sizeof(uint32_t), global_rkey_);
        // for(int i = 0; i < block_per_region; i++) {
        //     while(rkey_list[i] == (uint32_t)-1){
        //         remote_read(&rkey_list[i], sizeof(uint32_t), block_rkey_ + (region_index*block_per_region+i)*sizeof(uint32_t), global_rkey_);
        //     }
        // }
        // remote_write(new_rkey, sizeof(uint32_t)*block_per_region, block_rkey_ + region_index*block_per_region*sizeof(uint32_t), global_rkey_);
        return true;
    }

    bool fetch_class_region_rkey(uint32_t region_index, uint32_t* rkey_list) {
        remote_read(rkey_list, sizeof(uint32_t)*block_per_region, class_block_rkey_ + region_index*block_per_region*sizeof(uint32_t), global_rkey_);
        return true;
    }

    inline uint32_t get_section_class_index(uint32_t section_offset, uint32_t block_class) {return section_offset*block_class_num + block_class;};
    inline uint64_t get_section_region_addr(uint32_t section_offset, uint32_t region_offset) {return heap_start_ + section_offset*section_size_ + region_offset * region_size_ ;};
    inline uint64_t get_region_addr(uint32_t region_index) {return heap_start_ + region_index * region_size_;};
    inline uint64_t get_region_block_addr(uint32_t region_index, uint32_t block_offset) {return heap_start_ + region_index * region_size_ + block_offset * block_size_;} ;
    inline uint64_t get_block_addr(uint32_t block_offset) {return heap_start_ + block_offset * block_size_;} ;
    inline uint32_t get_region_block_rkey(uint32_t region_index, uint32_t block_offset) {
        uint32_t rkey; uint32_t rkey_new = -1;
        // do{
            remote_read(&rkey, sizeof(rkey), block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);
        // }while(rkey == -1);
        // if(rkey == -1){
        //     remote_rebind(get_region_block_addr(region_index, block_offset), 0, rkey);
        // }
        // remote_write(&rkey_new, sizeof(uint32_t), block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);
        // rkey_CAS = rkey;
        // if(!remote_CAS((uint32_t)-1, &rkey_CAS, block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_)){
        //     printf("rkey cas failed!\n");
        //     return 0;
        // }
        return rkey;
    };
    inline uint32_t get_block_rkey(uint32_t block_offset) {
        uint32_t rkey; uint32_t rkey_new = -1;
        // do{
            remote_read(&rkey, sizeof(rkey), block_rkey_ + (block_offset)*sizeof(uint32_t), global_rkey_);
        // }while(rkey == -1);
        // remote_write(&rkey_new, sizeof(uint32_t), block_rkey_ + (block_offset)*sizeof(uint32_t), global_rkey_);
        
        // if(!remote_CAS((uint32_t)-1, &rkey_CAS, block_rkey_ + (block_offset)*sizeof(uint32_t), global_rkey_)){
        //     printf("rkey cas failed!\n");
        //     return 0;
        // }
        return rkey;
    };
    inline uint32_t get_region_class_block_rkey(uint32_t region_index, uint32_t block_offset) {
        uint32_t rkey;
        remote_read(&rkey, sizeof(rkey), class_block_rkey_ + (region_index*block_per_region + block_offset)*sizeof(uint32_t), global_rkey_);
        return rkey;
    };
    
    bool init_region_class(region_e &alloc_region, uint32_t block_class, bool is_exclusive, uint32_t region_index);
    int fetch_region_block(region_e &alloc_region, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) ;
    int fetch_region_batch(region_e &alloc_region, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) ;
    bool fetch_region_class_block(region_e &alloc_region, uint32_t block_class, uint64_t &addr, uint32_t &rkey, bool is_exclusive, uint32_t region_index) ;
    int fetch_region_class_batch(region_e &alloc_region, uint32_t block_class, mr_rdma_addr* addr, uint64_t num, bool is_exclusive, uint32_t region_index) ;
    int free_region_block(uint64_t addr, bool is_exclusive) ;
    int free_region_batch(uint32_t region_offset, uint32_t free_bitmap, bool is_exclusive);

    int fetch_block(uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) ;
    bool fetch_block(uint16_t block_class, uint64_t &block_hint, uint64_t &addr, uint32_t &rkey) ;
    bool free_block(uint64_t addr) ;
    bool free_block(uint16_t block_class, uint64_t addr) ;

    private:

    struct ibv_mr *rdma_register_memory(void *ptr, uint64_t size);

    // << one-sided support functions >>
    // bool update_mem_metadata(uint64_t index);
    // bool update_mem_bitmap(uint64_t index);
    // bool update_rkey_metadata();
    // bool fetch_rkey_list_one_sided(uint64_t addr, uint32_t* rkey_list);

    // << one-sided read/write >>
    int rdma_remote_read(uint64_t local_addr, uint32_t lkey, uint64_t length,
                        uint64_t remote_addr, uint32_t rkey);

    int rdma_remote_write(uint64_t local_addr, uint32_t lkey, uint64_t length,
                            uint64_t remote_addr, uint32_t rkey);

    struct rdma_event_channel *m_cm_channel_;
    struct ibv_pd *m_pd_;
    struct ibv_cq *m_cq_;
    struct rdma_cm_id *m_cm_id_;
    uint64_t m_server_cmd_msg_;
    uint32_t m_server_cmd_rkey_;
    uint32_t m_fusee_rkey;
    uint32_t m_remote_size_;
    struct CmdMsgBlock *m_cmd_msg_;
    struct CmdMsgRespBlock *m_cmd_resp_;
    struct ibv_mr *m_msg_mr_;
    struct ibv_mr *m_resp_mr_;
    char *m_reg_buf_;
    struct ibv_mr *m_reg_buf_mr_;
    uint16_t conn_id_;

    // << one-sided support >>
    one_side_info m_one_side_info_;
    uint32_t global_rkey_;

      // basic info
    uint64_t block_size_;
    uint64_t block_num_;
    uint64_t region_size_;
    uint64_t region_num_;
    uint64_t section_size_;
    uint64_t section_num_;

    // info before heap segment
    uint64_t section_header_;
    uint64_t section_class_header_;
    uint64_t region_header_;
    uint64_t block_rkey_;
    uint64_t class_block_rkey_;
    uint64_t heap_start_;
    uint64_t block_header_;
    uint64_t backup_rkey_;
    // large_block_lockless block_;
    // uint32_t* rkey_list;
    // uint64_t last_alloc_;
    // uint64_t user_start_;
    // int total_old_= 0;

};

// static bool* full_bitmap;


}  // namespace kv
