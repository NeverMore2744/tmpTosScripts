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
    trace.loadProperty(propertyFileName);

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

      uint64_t fsMin = 1, fsMax = 1llu << 5;
      uint64_t nsMin = 1, nsMax = 1llu << 5;
      if (!freeMode) {
        fsMin = fsMax = fs;
        nsMin = nsMax = ns;
      }
      uint64_t totalCounts = 0;
      std::map<uint64_t, std::map<uint64_t, uint64_t>> fn2futureLbaCounts;
      std::map<uint64_t, std::queue<std::pair<uint64_t, uint64_t>>> f2addrQueue;
      std::map<uint64_t, uint64_t> fn2slc;

      // Used in two modes

      uint64_t addrNbAligned;

      // TEST
      std::set<uint64_t> timeSet;
      uint64_t reqCnt = 0;

      while (trace.readNextRequest(f, timestamp, type, offset, length) != -1) {
        if (beginTimestampInMin_ == 0) {
          beginTimestampInMin_ = timestamp / INTV_TO_MINUTE;
        }
        timeCalculated = timestamp / INTV_TO_MINUTE - beginTimestampInMin_;
        if (SELECT_7D) {
          if (timeCalculated >= MAX_MINUTE) continue;  // Add for all 
        }

        timeSet.insert(timeCalculated);
        timeCalculated = timestamp / 10000;
        // for write request
        if (type == 'R') continue;

        uint64_t begin = offset;
        uint64_t end = offset + length;
        offset = begin / 4096 * 4096;
        length = (end + 4095) / 4096 * 4096 - offset;

        offset /= 4096;
        length /= 4096;

        for (uint64_t lba = offset; lba < offset + length; lba++) {
          for (uint64_t fs = fsMin; fs <= fsMax; fs = fs << 1) {
            std::queue<std::pair<uint64_t, uint64_t>>& tq = f2addrQueue[fs];
            tq.push({lba, timeCalculated});

            std::vector<uint64_t> poppedLbas;
            while (!tq.empty() && timeCalculated - tq.front().second > fs) {
              uint64_t frontLba = tq.front().first;
              poppedLbas.push_back(frontLba);
              tq.pop();
            }

            for (uint64_t ns = nsMin; ns <= nsMax; ns = ns << 1) {
              uint64_t fn = fs * nsMax * 2 + ns;
              std::map<uint64_t, uint64_t>& tm = fn2futureLbaCounts[fn];

              addrNbAligned = lba / ns;
              for (uint64_t addr = (addrNbAligned > 0) ? (addrNbAligned - 1) : 0; addr <= addrNbAligned + 1; addr++) {
                if (tm.count(addr)) {
                  fn2slc[fn]++;
                  break;
                }
              }

              tm[addrNbAligned]++;
              for (auto & it : poppedLbas) {
                uint64_t poppedLbaAligned = it / ns;
                assert(tm.count(poppedLbaAligned));
                if (tm[poppedLbaAligned] == 1) {
                  tm.erase(poppedLbaAligned);
                } else {
                  tm[poppedLbaAligned]--;
                }
              }
            }
          }

          totalCounts++;
        } 
      }

      for (uint64_t fs = fsMin; fs <= fsMax; fs = fs << 1) 
      for (uint64_t ns = nsMin; ns <= nsMax; ns = ns << 1) {
        uint64_t fn = fs * nsMax * 2 + ns;
        std::cout << volumeId_ << " " << fs << " " << ns << " " << fn2slc[fn] << " " << totalCounts << "\n";
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
