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
  const uint32_t max_dis = (uint32_t)(1) << 29; // 128 GiB as maximum

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lba2updateDistancesIndex_;
//  LargeArray<uint64_t>* updateDistancesMap_;
  LargeArray<uint8_t>* frequencies_;
  LargeArray<uint32_t>* updateDistances_;

  std::string volumeId_;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 1;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

  uint32_t getDistance(uint64_t off) { // Return as number of MiB 
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 4096 / 16;  // 1MiB as Unit
    if (distance > max_dis - 1) distance = max_dis - 1;  // Max: 128 GiB
    return (uint32_t)distance;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    volumeId_ = volumeId;

    trace.loadProperty(propertyFileName);
    uint64_t maxLba = trace.getMaxLba(volumeId);
    uint64_t uniqueLba = trace.getUniqueLba(volumeId);
    nBlocks_ = maxLba + 1;

    std::cerr << "nBlocks = " << nBlocks_ << std::endl;
    std::cerr << "WsS = " << uniqueLba << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    lba2updateDistancesIndex_ = new LargeArray<uint64_t>(nBlocks_);

    frequencies_ = new LargeArray<uint8_t>(nBlocks_);
    updateDistances_ = new LargeArray<uint32_t>(uniqueLba * 4);
  }

  void analyze(char *inputTrace) {

    uint64_t offset, length, timestamp;
    char type;
    uint64_t timeCalculated;

    std::string line;
    std::filebuf fb;
    if (!fb.open(inputTrace, std::ios::in)) {
      std::cout << "Input file error: " << inputTrace << std::endl;
      return;
    }
    std::istream is(&fb);
    char line2[200];

    uint64_t wssCnt = 0, lba;

    while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
      if (type != 'W') continue;

      for (uint64_t i = 0; i < length; i += 1) {
        lba = offset + i;
        uint8_t freq = frequencies_->get(lba); // before this update, how many updates have happened
        uint64_t lastBlockId = indexMap_->get(lba);

        if (lastBlockId != 0) { // a block appeared before
          if (freq >= 4) { // has been updated four times
            continue;
          }

          uint32_t currDisInMiB = getDistance(lba);
          uint64_t udIndex = lba2updateDistancesIndex_->get(lba) - 4 + freq;
          updateDistances_->put(udIndex, currDisInMiB + 1);
        } else { // a new block
          lba2updateDistancesIndex_->put(lba, wssCnt + 4);
          wssCnt += 4;
        }

        if (freq == 4) { // fifth update, delete everything
          uint64_t udIndex = lba2updateDistancesIndex_->get(lba) - 4;
          for (uint64_t j = udIndex; j <= udIndex + 3; j++) {
            updateDistances_->put(j, 0);
          }
        }

        indexMap_->put(offset + i, currentId_++);
        if (lastBlockId > 0) frequencies_->put(offset + i, ((freq > 4) ? 5 : freq + 1));
      }
    }

    std::cerr << "finished, mapping ..." << std::endl;
    std::map<uint32_t, uint64_t> ud2udfreq;

    for (uint64_t i = 0; i < updateDistances_->getSize(); i++) {
      uint32_t value = updateDistances_->get(i);
      if (value == 0) continue;
      ud2udfreq[value]++;
    }

    for (uint64_t lba = 0; lba < frequencies_->getSize(); lba++) {
      uint8_t freq = frequencies_->get(lba);
      if (freq >= 5) continue;
      if (freq == 0) continue;
      ud2udfreq[getDistance(lba)]++;
    }

    std::cerr << "finished, writing ..." << std::endl;
    for (auto& it : ud2udfreq) {
      std::cout << it.first << " " << it.second << std::endl;
    }

//    updateDistances_->outputNonZero();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init("ali_open_property.txt", argv[2]);
  analyzer.analyze(argv[1]);
  return 0;
}
