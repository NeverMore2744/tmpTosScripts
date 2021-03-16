#include <iostream>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <map>
#include <cstdio>
#include <vector>
#include <set>
#include <algorithm>
#include "large_array.h"
#include "trace.h"

class Analyzer {

  Trace trace;

  // LBA to number of reads and number of writes
  std::set<uint64_t> lbas_;
  uint64_t maxLba = 0;

public:
  void analyze(char *inputTrace, char *volumeId)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;

    std::string line;
    std::filebuf fb;
    if (!fb.open(inputTrace, std::ios::in)) {
      std::cerr << "Input file error: " << inputTrace << std::endl;
      exit(1);
    }
    std::istream is(&fb);

    char line2[200];
    uint64_t cnt = 0;
    timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    std::cerr << "Volume: " << volumeId << std::endl;

    while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
      maxLba = std::max(maxLba, offset + length);
      for (uint64_t i = 0; i < length; i += 1) {
        lbas_.insert(offset + i);
      }
      if (++cnt % 10000000 == 0) {std::cerr << "request " << cnt << std::endl;}
    }

    std::cout << volumeId << " " << lbas_.size() << " " << maxLba << std::endl;
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.analyze(argv[2], argv[1]);
}
