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
  const uint32_t max_dis = 8192; // 128 GiB as maximum

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint32_t>* lastDistanceMap_;

  std::string volumeId_;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 1;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

  uint32_t getDistance(uint64_t off) { // Return as number of 16MiB 
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 4096;  // 16 MiB as Unit
    if (distance > max_dis - 1) distance = max_dis - 1;  // Max: 128 GiB
    return (uint32_t)distance;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    volumeId_ = volumeId;

    trace.loadProperty(propertyFileName, volume);
    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;

    std::cerr << "nBlocks = " << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    lastDistanceMap_ = new LargeArray<uint32_t>(nBlocks_);
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

    std::vector<uint64_t> u0values;
    std::vector<uint64_t> v0values;
    std::vector<uint64_t> v0u0denominator, v0u0numerator;

    for (int i = 4; i <= 9; i++) { // from 0.25 GiB to 8 GiB, in the units of 16 MiB
      u0values.push_back(1 << i);
      v0values.push_back(1 << i);
    }

    while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
      if (type != 'W') continue;

      for (uint64_t i = 0; i < length; i += 1) {
        if (indexMap_->get(offset + i) != 0) {
          uint32_t currDisIn16MiB = getDistance(offset + i);

          if (lastDistanceMap_->get(offset + i) != 0) {
            uint32_t lastDisIn16MiB = lastDistanceMap_->get(offset + i) - 1;

            for (int i = 0; i < (int)v0values.size(); i++) {
              for (int j = 0; j < (int)u0values.size(); j++) {
                int ij = i * u0values.size() + j;
                while (v0u0denominator.size() <= ij) {
                  v0u0denominator.push_back(0);
                  v0u0numerator.push_back(0);
                }

                uint64_t v0in16MiB = v0values[i], u0in16MiB = u0values[j];
                if (lastDisIn16MiB <= v0in16MiB) {
                  v0u0denominator[ij]++;
                  if (currDisIn16MiB <= u0in16MiB) {
                    v0u0numerator[ij]++;
                  }
                }
              }
            }
          }

          lastDistanceMap_->put(offset + i, currDisIn16MiB + 1);
        }

        indexMap_->put(offset + i, currentId_++);
      }
    }

    for (int i = 0; i < (int)v0values.size(); i++) {
      for (int j = 0; j < (int)u0values.size(); j++) {
        int ij = i * u0values.size() + j;
        uint64_t v0in16MiB = v0values[i], u0in16MiB = u0values[j];
        std::cout << volumeId_ << " " << 
          (double)v0in16MiB / 64 << " " <<
          (double)u0in16MiB / 64 << " " << 
          v0u0denominator[ij] << " " <<
          v0u0numerator[ij] << " " << 
          (double)v0u0numerator[ij] / (double)v0u0denominator[ij] << std::endl;
      }
    }

  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init("ali_open_property.txt", argv[2]);
  analyzer.analyze(argv[1]);
  return 0;
}
