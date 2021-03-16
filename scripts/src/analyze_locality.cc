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

      // Free mode 
      std::vector<uint64_t> fses({16, 32, 64}), nses({1,2});
      if (!freeMode) {
        fses = {(uint64_t)fs};
        nses = {(uint64_t)ns};
      }

      uint64_t fsMax = fses.size(), nsMax = nses.size(), fnMax = fsMax * nsMax;

      std::unordered_map<uint64_t, std::unordered_map<uint64_t, uint64_t>> fn2futureLbaCounts;
      std::unordered_map<uint64_t, std::queue<uint64_t>> f2addrQueue;
      std::vector<uint64_t> 
         fn2tLoc(fnMax, 0), fn2sLoc(fnMax, 0), fn2tLocPct(fnMax, 0), fn2sLocPct(fnMax, 0), fn2fLoc(fnMax, 0);
      uint64_t totalCounts = 0;

      // Used in two modes
      uint64_t addrNbAligned;

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
          for (uint64_t fsi = 0; fsi < fsMax; fsi++) {
            uint64_t fs = fses[fsi];

            std::queue<uint64_t>& tq = f2addrQueue[fs];
            tq.push(lba);

            for (uint64_t nsi = 0; nsi < nsMax; nsi++) {
              uint64_t ns = nses[nsi];
              uint64_t fn = nsi * fsMax + fsi;
//              std::cout << fsi << " " << nsi << " " << fn << "\n";

              std::unordered_map<uint64_t, uint64_t>& tm = fn2futureLbaCounts[fn];

              bool tExist = false, sExist = false, fExist = false;

//              addrNbAligned = lba / ns;
              for (uint64_t addr = (lba > ns) ? (lba - ns) : 0; addr <= lba + ns; addr++) {
                if (tm.count(addr)) {
                  uint64_t tCnt = tm[addr];

                  if (!fExist) {
                    fn2fLoc[fn]++;
                    fExist = true;
                  }

                  if (addr == lba) {
                    if (!tExist) { // temporal
                      fn2tLoc[fn]++;
                      tExist = true;
                    }
                    fn2tLocPct[fn] += tCnt;
                  } else {

                    if (!sExist) {  // spatial
                      fn2sLoc[fn]++;
                      sExist = true;
                    }
                    fn2sLocPct[fn] += tCnt;
                  }
                  
                }
              }

              tm[lba]++;

              if ((uint64_t)tq.size() > fs) {
                uint64_t frontLba = tq.front();

                assert(tm.count(frontLba));
                if (tm[frontLba] == 1) {
                  tm.erase(frontLba);
                } else {
                  tm[frontLba]--;
                }
              }

              assert(tm.size() <= fs);
            }
            if (tq.size() > fs) {
              tq.pop(); 
            }
          }
          totalCounts++;
        }
      }

      for (uint64_t fsi = 0; fsi < fsMax; fsi++)
      for (uint64_t nsi = 0; nsi < nsMax; nsi++) {
        uint64_t fs = fses[fsi];
        uint64_t ns = nses[nsi];
        uint64_t fn = nsi * fsMax + fsi;
        std::cout << volumeId_ << " " << fs << " " << ns << " " << fn2tLoc[fn] << " " << totalCounts << " t_loc_exi\n";
        std::cout << volumeId_ << " " << fs << " " << ns << " " << fn2tLocPct[fn] / fs << " " << totalCounts << " t_loc_pct\n";
        std::cout << volumeId_ << " " << fs << " " << ns << " " << fn2sLoc[fn] << " " << totalCounts << " s_loc_exi\n";
        std::cout << volumeId_ << " " << fs << " " << ns << " " << fn2sLocPct[fn] / fs << " " << totalCounts << " s_loc_pct\n";
        std::cout << volumeId_ << " " << fs << " " << ns << " " << fn2fLoc[fn] << " " << totalCounts << " f_loc_exi\n";
        std::cout << volumeId_ << " " << fs << " " << ns << " " << (fn2tLocPct[fn] + fn2sLocPct[fn]) / fs << " " << totalCounts << " f_loc_pct\n";
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
