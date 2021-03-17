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

#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;
  std::map<uint64_t, std::pair<uint64_t, uint64_t>> reqSize_;

public:

  void analyze(const char *inputTrace, const char* volume)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;

    std::string line;
    std::filebuf fb;
    if (!fb.open(inputTrace, std::ios::in)) {
      std::cout << "Input file error: " << inputTrace << std::endl;
      return;
    }
    std::istream is(&fb);

    char line2[200];
    uint64_t cnt = 0;
    
    trace.myTimer(true, "request sizes");

    while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2, true)) {
      if (isWrite == 'R') { // read request
        reqSize_[length].first += 1;
      } else if (isWrite == 'W') { // write request
        reqSize_[length].second += 1;
      }
      trace.myTimer(false, "request sizes");
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
