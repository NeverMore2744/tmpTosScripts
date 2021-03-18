#include <iostream>
#include <unordered_map>
#include <fstream>
#include <string>
#include <cstdint>
#include <map>
#include <cstdio>
#include <vector>
#include <set>
#include <algorithm>
#include "large_array.h"
#include "trace.h"
#include <sys/time.h>

#define MAX_MINUTE (7 * 24 * 60)

class Analyzer : Analyzer_base {
  std::map<uint64_t, std::pair<uint64_t, uint64_t>> reqSize_;

public:

  void analyze(const char *inputTrace, const char* volume)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;
    openTrace(inputTrace);

    trace_.myTimer(true, "request sizes");
    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_, true)) {
      if (!isWrite) { // read request
        reqSize_[length].first += 1;
      } else if (isWrite) { // write request
        reqSize_[length].second += 1;
      }
      trace_.myTimer(false, "request sizes");
    }

    std::cout << reqSize_.size() << std::endl;
    for (auto pr : reqSize_) {
      std::cout << volume << " " << pr.first * 512 
        << " " << pr.second.first << " " << pr.second.second << std::endl;
    }
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.analyze(argv[2], argv[1]);
}
