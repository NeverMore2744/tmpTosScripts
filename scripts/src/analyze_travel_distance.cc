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

#define INTV (600000000 / 60)
#define SPACE_INTV 32  // 32 blocks is the recent lba
#define RECENT_REQ_THRES 32 // 32 requests is the recent request

#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;

  LargeArray<uint64_t>* travelDistanceHistogram_;

  uint64_t startTimestamp_;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

  uint64_t getLbaDistance(uint64_t lba1, uint64_t lba2) {
    uint64_t distance = (lba1 < lba2) ? lba2 - lba1 : lba1 - lba2;
    distance /= SPACE_INTV;
    if (distance >= nBlocks_ / 2 / SPACE_INTV) distance = nBlocks_ / 2 / SPACE_INTV;
    return distance;
  }

public:

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
    std::cout << nBlocks_ << std::endl;

    travelDistanceHistogram_ = new LargeArray<uint64_t>(nBlocks_ / 2 / SPACE_INTV);
  }

  void analyze(char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    char type;
    uint64_t timeCalculated;

    std::vector<uint64_t> nextLbas;
    int nextPtr = 0;

    FILE *f = fopen(inputTrace, "r");
    int tmpCnt = 0;

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

      uint64_t minDis = nBlocks_, dis;

      if (true) { // all request
        if (nextLbas.size() < RECENT_REQ_THRES) {
          nextLbas.push_back(offset);
          nextPtr = 0;
        } else {
          minDis = nBlocks_;
          for (int k = 0; k < (int)nextLbas.size(); k++) {
            dis = getLbaDistance(offset, nextLbas[k]);
            minDis = (minDis > dis) ? dis : minDis;
          }
          travelDistanceHistogram_->inc(minDis);

          nextLbas[nextPtr] = offset;
          nextPtr = (nextPtr + 1) % RECENT_REQ_THRES;
        }
      }
    }
    travelDistanceHistogram_->output();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
