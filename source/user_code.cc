/*
 * @Author: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @Date: 2023-09-13 22:28:40
 * @LastEditors: Blahaj Wang && wxy1999@mail.ustc.edu.cn
 * @LastEditTime: 2023-09-13 22:32:37
 * @FilePath: /rmalloc_newbase/source/user_code.cc
 * @Description: 
 * 
 * Copyright (c) 2023 by wxy1999@mail.ustc.edu.cn, All Rights Reserved. 
 */

#include <bits/stdint-uintn.h>
#include "cpu_cache.h"

const uint64_t cache_size = 1024*1024*2;

int main(int argc, char** argv){
    mralloc::cpu_cache cpu_cache_ = mralloc::cpu_cache(cache_size);
    int k=1000;
    while(k--)
        cpu_cache_.fetch_cache(1);
    return 0;
}