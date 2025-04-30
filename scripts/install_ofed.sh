#!/bin/bash

#xl170

sudo apt update
sudo apt install -y cmake libboost-all-dev libgoogle-perftools-dev libtbb-dev libgtest-dev libgflags-dev libgoogle-glog-dev

sudo apt-get install lsb-release curl gpg
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
sudo chmod 644 /usr/share/keyrings/redis-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
sudo apt-get update
sudo apt-get install -y redis

git clone https://github.com/memkind/memkind.git
cd memkind
./autogen.sh
./configure --prefix=/usr
make
sudo make install
cd ..; rm -rf memkind

wget https://github.com/redis/hiredis/archive/master.zip
unzip master.zip
cd hiredis-master
make; sudo make install
mkdir /usr/lib/hiredis
cp /usr/local/lib/libhiredis.so.1.3.0 /usr/lib/hiredis/
mkdir /usr/include/hiredis
cp /usr/local/include/hiredis/hiredis.h /usr/include/hiredis/
echo '/usr/local/lib' >>;>>;/etc/ld.so.conf
ldconfig

nohup redis-server --bind 10.10.1.1 --port 2222 --protected-mode no

wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-7.1.0.0/MLNX_OFED_LINUX-4.9-7.1.0.0-ubuntu20.04-x86_64.tgz
tar -zxvf MLNX_OFED_LINUX-4.9-7.1.0.0-ubuntu20.04-x86_64.tgz
cd MLNX_OFED_LINUX-4.9-7.1.0.0-ubuntu20.04-x86_64
sudo ./mlnxofedinstall --force
cd ../; sudo rm -rf MLNX_OFED_LINUX-4.9-7.1.0.0-ubuntu20.04-x86_64*

sudo sed -i "s/GRUB_CMDLINE_LINUX_DEFAULT=\"/GRUB_CMDLINE_LINUX_DEFAULT=\"iommu=pt/" /etc/default/grub
sudo grub-mkconfig -o /boot/grub/grub.cfg
sudo /etc/init.d/openibd restart

sudo mst start
sudo mlxconfig -y -d /dev/mst/mt4117_pciconf0 set SRIOV_EN=1 NUM_OF_VFS=16
sudo mlxconfig -y -d /dev/mst/mt4117_pciconf1 set SRIOV_EN=1 NUM_OF_VFS=16

sudo reboot
