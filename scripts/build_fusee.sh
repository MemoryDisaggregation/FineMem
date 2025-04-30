cd ~/FineMem/applications
git clone https://github.com/MemoryDisaggregation/FUSEE_FineMem.git
cd FUSEE_FineMem/
cp ~/FineMem/include/cpu_cache.h ./src/
cp ~/FineMem/include/free_block_manager.h ./src/
cp ~/FineMem/include/msg.h ./src/
cp ~/FineMem/include/rdma_conn* ./src/
cp ~/FineMem/build/source/libmralloc.a ./lib/
mkdir build; cd build;
cmake ..; make
cd ..; ./download_workload.sh
cd ../; mkdir build/ycsb-test/workloads
cp ./setup/workloads/* ./build/ycsb-test/workloads/
cp ./ycsb-test/split-workload.py ./build/ycsb-test/