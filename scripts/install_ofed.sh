#!/bin/bash

#xl170

sudo apt update

sudo apt install -y cmake libboost-all-dev libgoogle-perftools-dev libtbb-dev libgtest-dev libgflags-dev libgoogle-glog-dev

git clone https://github.com/memkind/memkind.git

cd memkind

./autogen.sh

./configure --prefix=/usr

make

sudo make install

cd ..; rm -rf memkind

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
