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
#include <string>
#include <sys/time.h>

class Analyzer {
  Trace trace;

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lastTimestamp_;
  LargeArray<char>* lastState_;

  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
//    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } war_;
  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
//    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } rar_;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;

  uint64_t getDistance(uint64_t off) {
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 256;
    if (distance >= nBlocks_ * 8 / 256) distance = nBlocks_ * 8 / 256;
    return distance;
  }

  uint64_t getTime(uint64_t off, uint64_t timestamp) {
    if (timestamp < lastTimestamp_->get(off)) {
      return -1llu;
    }
    uint64_t diff = timestamp - lastTimestamp_->get(off);
    diff /= 10000000;
    return diff;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
    std::cout << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    lastTimestamp_ = new LargeArray<uint64_t>(nBlocks_);
    lastState_ = new LargeArray<char>(nBlocks_);

    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    war_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 3600);
//    war_.intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);

    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    rar_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 3600);
//    rar_.intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);
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
      bool first = true;
      uint64_t cnt = 0, lastReqTimestamp;
      timeval tv1, tv2;
      trace.myTimer(true, "RAR and WAR");

      while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
        if (first) {
          first = false;
        } else if (timestamp < lastReqTimestamp) {
          continue;
        }
        lastReqTimestamp = timestamp;

        if (!isWrite) { // read request
          for (uint64_t i = 0; i < length; i += 1) {
            uint8_t last = lastState_->get(offset + i);
            if (last == 'R') {
//              rar_.intervalHistogramByDataAmount_->inc(getDistance(offset + i));
              rar_.intervalHistogramByTime_->inc(getTime(offset + i, timestamp));
            }

            lastState_->put(offset + i, 'R');
            lastTimestamp_->put(offset + i, timestamp);
          }
        } else { // write request
          for (uint64_t i = 0; i < length; i += 1) {
            uint8_t last = lastState_->get(offset + i);
            if (last == 'R') {
//              war_.intervalHistogramByDataAmount_->inc(getDistance(offset + i));
              war_.intervalHistogramByTime_->inc(getTime(offset + i, timestamp));
            }

            lastState_->put(offset + i, 'W');
            lastTimestamp_->put(offset + i, timestamp);
            indexMap_->put(offset + i, currentId_++);
          }
        }

        trace.myTimer(false, "RAR and WAR");
      }

      std::cerr << "Output: rar time" << std::endl;
      rar_.intervalHistogramByTime_->outputNonZero();
//      std::cerr << "Output: rar data amount" << std::endl;
//      rar_.intervalHistogramByDataAmount_->outputNonZero();
      std::cerr << "Output: war time" << std::endl;
      war_.intervalHistogramByTime_->outputNonZero();
//      std::cerr << "Output: war data amount" << std::endl;
//      war_.intervalHistogramByDataAmount_->outputNonZero();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
