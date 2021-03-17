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
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

public:

  void analyze(char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    char type;
    uint64_t timeCalculated;

    std::string line;
    std::filebuf fb;
    if (!fb.open(inputTrace, std::ios::in)) {
      std::cout << "Input file error: " << inputTrace << std::endl;
      return;
    }
    std::istream is(&fb);

    timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    char line2[200];
    uint64_t cnt = 0;

    while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2, true)) {
      if (type == 'R') { // read request
        reqSize_[length].first += 1;
      } else if (type == 'W') { // write request
        reqSize_[length].second += 1;
      }

      cnt++;
      if (cnt % 100000 == 0) {
        gettimeofday(&tv2, NULL);
        std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
      }
    }

    std::cout << reqSize_.size() << std::endl;
    for (auto pr : reqSize_) {
      std::cout << pr.first * 512 << " " << pr.second.first << " " << pr.second.second << std::endl;
    }
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.analyze(argv[1]);
}