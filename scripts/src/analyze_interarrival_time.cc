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
#include <sys/time.h>

#define INTV_TO_MINUTE 600000000  
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;
  LargeArray<uint64_t>** intervalHistograms_;

  // diff is in 1e-7 seconds
  void updateInterArrivalTime(uint64_t diff) {
    uint64_t secondMult10 = diff / 1000000;
    uint64_t index, value;
    if (secondMult10 < 1) {
      index = 0, value = diff;
    } else if (secondMult10 < 10) {
      index = 1, value = diff / 10;
    } else if (secondMult10 < 100) {
      index = 2, value = diff / 1000;
    } else if (secondMult10 < 1000) {
      index = 3, value = diff / 100000;
    } else if (secondMult10 < 10000) {
      index = 4, value = secondMult10;
    } else {
      index = 5, value = secondMult10 / 10;
    }

    intervalHistograms_[index]->inc(value);
  }

public:
  void analyze(char* inputTrace) {
      intervalHistograms_ = new LargeArray<uint64_t>*[6];
      intervalHistograms_[0] = new LargeArray<uint64_t>(1 * 1000000); // 0-0.1s, by 10^-7
      intervalHistograms_[1] = new LargeArray<uint64_t>(10 * 100000); // 0.1-1s, by 10^-6
      intervalHistograms_[2] = new LargeArray<uint64_t>(100 * 1000); // 1-10s, by 10^-5
      intervalHistograms_[3] = new LargeArray<uint64_t>(1000 * 10); // 10-100s, by 10^-3
      intervalHistograms_[4] = new LargeArray<uint64_t>(10000 * 1); // 100-1000s, by 10^-1
      intervalHistograms_[5] = new LargeArray<uint64_t>(31 * 24 * 3600); // > 1000s, by 1s

      uint64_t offset, length, timestamp;
      uint64_t prevTimestamp = -1ull;
      uint64_t timeCalculated;
      bool isWrite;

      std::string line;
      std::filebuf fb;
      if (!fb.open(inputTrace, std::ios::in)) {
        std::cout << "Input file error: " << inputTrace << std::endl;
        exit(1);
      }
      std::istream is(&fb);

      char line2[200];
      uint64_t cnt = 0;
      trace.myTimer(true, "interarrival time");

      while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
        if (prevTimestamp != -1ull) {
          if (timestamp < prevTimestamp) {
            continue;
          }
          updateInterArrivalTime(timestamp - prevTimestamp);
          prevTimestamp = timestamp;
        } else {
          prevTimestamp = timestamp;
        }

        trace.myTimer(false, "interarrival time");
      }

      for (int i = 0; i < 6; ++i) {
        intervalHistograms_[i]->outputNonZero();
      }
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.analyze(argv[2]);
}
