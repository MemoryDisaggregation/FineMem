
#include <cstdio>
#include <string>
#include <iostream>
#include "memory_node.h"

int main(int argc, char *argv[]) {
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << "<device> <ip> <port>" << std::endl;
    return 0;
  }

  std::string device = argv[1];
  std::string ip = argv[2];
  std::string port = argv[3];
  mralloc::MemoryNode *heap = new mralloc::MemoryNode(true);
  heap->start(ip, port, device);

  while (getchar()) {
    heap->print_alloc_info();
  }

  getchar();
  heap->stop();
  delete heap;
  return 0;
}