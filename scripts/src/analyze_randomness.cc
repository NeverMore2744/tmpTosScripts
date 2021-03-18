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

#define MAX_MINUTE (7 * 24 * 60)

class Analyzer : Analyzer_base {

  std::string volumeId_;
  uint64_t nBlocks_ = -1ull;

  uint64_t getLbaDistance(uint64_t lba1, uint64_t lba2) {
    uint64_t distance = (lba1 < lba2) ? lba2 - lba1 : lba1 - lba2;
    if (distance >= nBlocks_ / 2) {
      distance = nBlocks_ / 2;
    }
    return distance;
  }

public:

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    volumeId_ = volumeId;
    trace_.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace_.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
  }

  void analyze(char *inputTrace, uint64_t recentRequestNumber, uint64_t windowSize)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;
    std::vector<uint64_t> nextLbas;
    int nextPtr = 0;
    openTrace(inputTrace);

    trace_.myTimer(true, "randomness");
    uint64_t ansRandom = 0, ansAll = 0;

    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      uint64_t minDis = nBlocks_, dis;

      if (nextLbas.size() < recentRequestNumber) {
        nextLbas.push_back(offset);
        nextPtr = 0;
      } else {
        minDis = nBlocks_;
        for (int k = 0; k < (int)nextLbas.size(); k++) {
          dis = getLbaDistance(offset, nextLbas[k]);
          minDis = (minDis > dis) ? dis : minDis;
        }

        if (minDis < windowSize) ansRandom++;
        ansAll++;

        nextLbas[nextPtr] = offset;
        nextPtr = (nextPtr + 1) % recentRequestNumber;

        trace_.myTimer(false, "randomness");
      }
    }
    std::cout << volumeId_ << " " << recentRequestNumber << " " << windowSize << " " << ansRandom << " " << ansAll << " " << std::endl;
  }
};

int main(int argc, char *argv[]) {
  uint64_t recentRequestNumber, windowSize;
  if (argc < 5) {
    std::cerr << "Input error: <volume> <trace file> <property> <recentRequestNumber> <windowSize>" 
      << std::endl;
    return 1;
  }
  sscanf(argv[4], "%lu", &recentRequestNumber);
  sscanf(argv[5], "%lu", &windowSize);

  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2], recentRequestNumber, windowSize);
}
