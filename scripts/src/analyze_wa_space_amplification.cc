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

  LargeArray<uint64_t>* indexMap_;

  uint64_t startTimestamp_;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

public:

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
//    std::cout << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
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

    printf("t0 t EX_avg EY_avg\n");
    uint64_t nValidBlocks = 0;
    uint64_t lba, ud, maxLba = 1024 * 1024;
    uint64_t cnt = 0;

    std::vector<uint64_t> t0Values;
    std::vector<uint64_t> Tvalues;

    for (int i = 2; i <= 64; i <<= 1) {
      t0Values.push_back(maxLba / i);
      Tvalues.push_back(maxLba / i * 2);
    }

    std::vector<uint64_t> exValues;
    std::vector<uint64_t> eyValues;
    std::vector<uint64_t> hotBlocks;
    std::vector<uint64_t> countsGc;
    std::vector<std::set<uint64_t>> hotBlockSets;

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

      for (int i = offset; i < offset + length; i++) {
        uint64_t lba = i;

        cnt++;

        if (indexMap_->get(lba) == 0) {
          ud = 0;
          nValidBlocks++;
        } else {
          ud = cnt - indexMap_->get(lba);
        }
        indexMap_->put(lba, cnt);

        for (int i = 0; i < (int)Tvalues.size(); i++) {
          for (int j = 0; j < (int)t0Values.size(); j++) {
            int ij = i * t0Values.size() + j;

            if (hotBlockSets.size() <= ij) {
              hotBlocks.push_back(0);
              hotBlockSets.push_back({});
              exValues.push_back(0);
              eyValues.push_back(0);
              countsGc.push_back(0);
            }

            uint64_t t = Tvalues[i], t0 = t0Values[j];

            if (cnt % t == 0) { // GC
              // Record information for space amplification
              exValues[ij] += hotBlockSets[ij].size();
              eyValues[ij] += hotBlocks[ij];
              countsGc[ij]++;

              hotBlocks[ij] = 0;
              hotBlockSets[ij].clear();
            }

            if (ud > 0 && ud < t0) { // Data separation
              hotBlockSets[ij].insert(lba);
              hotBlocks[ij]++;
            } else {
              if (hotBlockSets[ij].count(lba)) {
                hotBlockSets[ij].erase(lba);
              }
            }
          }
        }
      }
    }

    for (int i = 0; i < (int)Tvalues.size(); i++) {
      for (int j = 0; j < (int)t0Values.size(); j++) {
        int ij = i * t0Values.size() + j;
        printf("%d %d %lld %lld\n", t0Values[j], Tvalues[i], exValues[ij] / (countsGc[ij] + 1), eyValues[ij] / (countsGc[ij] + 1));
      }
    }
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init("device_property_translated.txt", argv[2]);
  analyzer.analyze(argv[1]);
}
