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

#include <cassert>

#define INTV (10000000 * 60)
#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;
  const uint32_t max_dis = 8192;

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint32_t>* lastDistanceMap_;
  LargeArray<uint64_t>* intervalHistogramByDataAmount_;

  LargeArray<uint64_t>* counter_;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 1;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

  uint32_t getDistance(uint64_t off) {
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 4096;  // 16 MiB 
//    if (distance >= nBlocks_ * 8 / 256) distance = nBlocks_ * 8 / 256;
    if (distance > max_dis - 1) distance = max_dis - 1;  // Max: 128 GiB
    return (uint32_t)distance;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
//    std::cout << "nBLocks:" << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    lastDistanceMap_ = new LargeArray<uint32_t>(nBlocks_);
    intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(8192 * 8192);
  }

  void analyze(char *inputTrace) {
      uint64_t offset, length, timestamp;
      char type;
      uint64_t timeCalculated;

      FILE *f = fopen(inputTrace, "r");
      std::map<uint64_t, uint64_t> sums;
      std::set<uint64_t> lbas;

      while (trace.readNextRequest(f, timestamp, type, offset, length) != -1) {
        timeCalculated = timestamp / INTV_TO_MINUTE - beginTimestampInMin_;
        if (SELECT_7D) {
          if (timeCalculated >= MAX_MINUTE) continue;  // Add for all 
        }

        if (startTimestamp_ == 0) {
          startTimestamp_ = timestamp;
        }

        // for write request
        uint64_t begin = offset;
        uint64_t end = offset + length;
        offset = begin / 4096 * 4096;
        length = (end + 4095) / 4096 * 4096 - offset;

        offset /= 4096;
        length /= 4096;

        if (type == 'W') { // write request
          for (uint64_t i = 0; i < length; i += 1) {
            if (indexMap_->get(offset + i) != 0) {
              uint64_t currDis = getDistance(offset + i);
              if (currDis > 64llu * 1024 * 1024) {
                currDis = 64llu * 1024 * 1024;
              }
              sums[currDis]++;
            }

            indexMap_->put(offset + i, currentId_++);
          }
        }
      }

      std::map<uint64_t, uint64_t> sumsCumulative;
      uint64_t sumValue = 0;
      for (auto& it : sums) {
        sumValue += it.second;
        sumsCumulative[it.first] = sumValue;
      }

      // Not completed
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init("device_property_translated.txt", argv[2]);
 // analyzer.analyze(argv[2]);
  analyzer.analyze(argv[1]);
  return 0;
}
