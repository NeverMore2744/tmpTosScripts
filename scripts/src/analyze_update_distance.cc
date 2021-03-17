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

class Analyzer {
  Trace trace;

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lastTimestamp_;

  LargeArray<uint64_t>* intervalHistogramByTime_;
  LargeArray<uint64_t>* intervalHistogramByDataAmount_;

  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 1;

  uint64_t getDistance(uint64_t off) {
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 256;
    if (distance >= nBlocks_ * 8 / 256) distance = nBlocks_ * 8 / 256;
    return distance;
  }

  uint64_t getTimeInMin(uint64_t off, uint64_t timestamp) {
    uint64_t diff = timestamp - lastTimestamp_->get(off);
    diff /= 10000000 * 60;
    return diff;
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
    lastTimestamp_ = new LargeArray<uint64_t>(nBlocks_);
    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    intervalHistogramByTime_ = new LargeArray<uint64_t>(9 * 24 * 60);
    intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);
  }

  void analyze(char *inputTrace) {
      uint64_t offset, length, timestamp;
      bool isWrite;

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

      while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
        if (first) {
          first = false;
        } else if (timestamp < lastReqTimestamp) {
          continue;
        }
        lastReqTimestamp = timestamp;

        if (isWrite) { // write request
          for (uint64_t i = 0; i < length; i += 1) {
            if (indexMap_->get(offset + i) != 0) {
              intervalHistogramByDataAmount_->inc(getDistance(offset + i));

              if (getTimeInMin(offset + i, timestamp) >= intervalHistogramByTime_->getSize()) {
                intervalHistogramByTime_->inc(intervalHistogramByTime_->getSize() - 1);
              }
              intervalHistogramByTime_->inc(getTimeInMin(offset + i, timestamp));
            }

            lastTimestamp_->put(offset + i, timestamp);
            indexMap_->put(offset + i, currentId_++);
          }
        }

        cnt++;
        if (cnt % 1000000 == 0) {
          gettimeofday(&tv2, NULL);
          std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
        }
      }

      intervalHistogramByTime_->outputNonZero();
      intervalHistogramByDataAmount_->outputNonZero();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}