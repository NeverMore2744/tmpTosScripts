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

class Analyzer : Analyzer_base {

  LargeArray<uint64_t>* travelDistanceHistogram_;

  uint64_t getLbaDistance(uint64_t lba1, uint64_t lba2) {
    uint64_t distance = (lba1 < lba2) ? lba2 - lba1 : lba1 - lba2;
    distance /= SPACE_INTV;
    if (distance >= nBlocks_ / 2 / SPACE_INTV) distance = nBlocks_ / 2 / SPACE_INTV;
    return distance;
  }

public:

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace_.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace_.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
    std::cout << nBlocks_ << std::endl;

    travelDistanceHistogram_ = new LargeArray<uint64_t>(nBlocks_ / 2 / SPACE_INTV);
  }

  void analyze(char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;

    std::vector<uint64_t> nextLbas;
    int nextPtr = 0;
    int tmpCnt = 0;

    openTrace(inputTrace);
    trace_.myTimer(true, "travel distances");

    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      uint64_t minDis = nBlocks_, dis;

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

      trace_.myTimer(false, "travel distances");
    }
    travelDistanceHistogram_->output();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
