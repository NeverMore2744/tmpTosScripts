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

#include <cassert>

#define INTV (10000000 * 60)
#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* intervalHistogramByDataAmount_;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 1;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

  uint64_t getDistance(uint64_t off) {
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 256;
    if (distance >= nBlocks_ * 8 / 256) distance = nBlocks_ * 8 / 256;
    return distance;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
    std::cout << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);
  }

  void analyze(char *inputTrace) {
      uint64_t offset, length, timestamp;
      char type;

      {
        indexMap_ = new LargeArray<uint64_t>(nBlocks_);
        intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);
      }

      std::string line;
      std::filebuf fb;
      if (!fb.open(inputTrace, std::ios::in)) {
        std::cout << "Input file error: " << inputTrace << std::endl;
        exit(1);
      }
      std::istream is(&fb);

      char line2[200];
      uint64_t cnt = 0, dis, delTime;
      timeval tv1, tv2;
      gettimeofday(&tv1, NULL);

      bool first = true;
      uint64_t lastReqTimestamp;

      while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
        if (first) {
          first = false;
        } else if (timestamp < lastReqTimestamp) {
          continue;
        }
        lastReqTimestamp = timestamp;

        if (type != 'W') {
          continue;
        }

        for (uint64_t i = 0; i < length; i += 1) {
          if (indexMap_->get(offset + i) != 0) {
            intervalHistogramByDataAmount_->inc(getDistance(offset + i));
          }

          indexMap_->put(offset + i, currentId_++);
        }

        cnt++;
        if (cnt % 100000 == 0) {
          gettimeofday(&tv2, NULL);
          std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
        }
      }

      for (uint64_t i = 0; i < indexMap_->getSize(); i++) {
        if (indexMap_->get(i) != 0) {
          intervalHistogramByDataAmount_->inc(getDistance(i));
        }
      }

      intervalHistogramByDataAmount_->outputNonZero();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[2], argv[3]);
  analyzer.analyze(argv[1]);
}
