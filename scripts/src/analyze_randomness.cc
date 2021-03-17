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

#define INTV (600000000 / 60)
#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;

  std::string volumeId_;
  uint64_t startTimestamp_;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;

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
    trace.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
  }

  void analyze(char *inputTrace, uint64_t recentRequestNumber, uint64_t windowSize)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;
    std::vector<uint64_t> nextLbas;
    int nextPtr = 0;

    std::string line;
    std::filebuf fb;
    if (!fb.open(inputTrace, std::ios::in)) {
      std::cout << "Input file error: " << inputTrace << std::endl;
      exit(1);
    }
    std::istream is(&fb);

    char line2[200];
    uint64_t cnt = 0;
    timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    int tmpCnt = 0;

    uint64_t ansRandom = 0, ansAll = 0;

    while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
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

        cnt++;
        if (cnt % 1000000 == 0) {
          gettimeofday(&tv2, NULL);
          std::cerr << "Volume " << volumeId_ << ": " 
            << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
        }
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
