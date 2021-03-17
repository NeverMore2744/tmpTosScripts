#include <iostream>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <map>
#include <cstdio>
#include <queue>
#include <vector>
#include <set>
#include <algorithm>
#include "large_array.h"
#include "trace.h"
#include <string>
#include <cassert>

#define INTV_TO_MINUTE 60000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;

  LargeArray<uint64_t>* indexMap_;
  std::string volumeId_;

  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } war_;
  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } rar_;

  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;
  uint64_t beginTimestampInMin_ = 0; // 12816637200llu / 60; // Minimum timestamp in MSR 

public:

  // initialize properties
  void init(char *propertyFileName, const char *volume) {
    std::string volumeId(volume);
    volumeId_ = volumeId;
    trace.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
//    std::cout << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
  }

  // orig: ns (neighborSize) = 8, fs (futureSize) = 512
  void analyze(char *inputTrace, int ns = 8, int fs = 512, bool freeMode = true) {
      uint64_t offset, length, timestamp;
      char type;
      uint64_t timeCalculated;

      FILE *f = fopen(inputTrace, "r");
      if (f == NULL) {
        printf("File not exist! %s", inputTrace);
        exit(1);
      }

      uint64_t lastLba = 0;
      bool first = true;
      std::vector<uint64_t> lbas(ns*2+1, 0);
      uint64_t totalCounts = 0;

      while (trace.readNextRequest(f, timestamp, type, offset, length) != -1) {
        if (beginTimestampInMin_ == 0) {
          beginTimestampInMin_ = timestamp / INTV_TO_MINUTE;
        }
        timeCalculated = timestamp / INTV_TO_MINUTE - beginTimestampInMin_;
        if (SELECT_7D) {
          if (timeCalculated >= MAX_MINUTE) continue;  // Add for all 
        }

        //printf("%llu %llu %llu\n", timestamp, offset, length);
        // for write request
        if (type == 'R') continue;

        uint64_t begin = offset;
        uint64_t end = offset + length;
        offset = begin / 4096 * 4096;
        length = (end + 4095) / 4096 * 4096 - offset;

        offset /= 4096;
        length /= 4096;

        for (uint64_t lba = offset; lba < offset + length; lba++) {
          if (!first) {
            if (lba >= lastLba && lba - lastLba <= ns) {
              lbas[lba - lastLba + ns]++;
            } else if (lba < lastLba && lastLba - lba <= ns) {
              lbas[ns - lastLba + lba]++;
            }
          } else {
            first = false;
          }
          lastLba = lba;
        }
        totalCounts += length;
      }

      for (int i = 0; i < (int)lbas.size(); i++) {
        std::cout << volumeId_ << " " << (i - ns) << " " << lbas[i] << " " << totalCounts << std::endl; 
      }
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  uint64_t futureSize, neighborSize;
  
  if (argc < 3) {
    printf("Error: parameter not enough\n");
    printf("   %s <trace_file> <device_name> <futureSize> <neighborSize>\n", argv[0]);
    return 1;
  }
  analyzer.init("device_property_translated.txt", argv[2]);

  if (argc >= 5) {
    sscanf(argv[3], "%llu", &futureSize);
    sscanf(argv[4], "%llu", &neighborSize);
  // analyzer.analyze(argv[2]);
    analyzer.analyze(argv[1], neighborSize, futureSize, false);
  } else {
    analyzer.analyze(argv[1]);
  }
  return 0;
}
