#include <iostream>
#include <unordered_map>
#include <sys/time.h>
#include <string>
#include <cstdint>
#include <map>
#include <cstdio>
#include <vector>
#include <set>
#include <algorithm>
#include "large_array.h"
#include "trace.h"

class Analyzer {

  // LBA to number of reads and number of writes
  Trace trace;
  LargeArray<uint64_t> *readReqs_;
  LargeArray<uint64_t> *writeReqs_;
  LargeArray<uint64_t> *readTraffic_;
  LargeArray<uint64_t> *writeTraffic_;
  LargeArray<uint64_t> *updateTraffic_;
  uint64_t maxLba;

public:
  Analyzer() {
    readReqs_ = new LargeArray<uint64_t>(31*24*60+1);
    writeReqs_ = new LargeArray<uint64_t>(31*24*60+1);
    readTraffic_ = new LargeArray<uint64_t>(31*24*60+1);
    writeTraffic_ = new LargeArray<uint64_t>(31*24*60+1);
    updateTraffic_ = new LargeArray<uint64_t>(31*24*60+1);
  }

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName);
    maxLba = trace.getMaxLba(volumeId);
  }

  void analyze(char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;

    uint64_t cnt = 0;
    uint64_t timeCalculated;

    std::string line;
    std::filebuf fb;
    if (!fb.open(inputTrace, std::ios::in)) {
      std::cout << "Input file error: " << inputTrace << std::endl;
      exit(1);
    }
    std::istream is(&fb);

    char line2[200];
    timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    int size_array = trace.getMaxLba(; // 1024 * 1024 * 4;
    uint64_t* wwss = new uint64_t[size_array];
    memset(wwss, 0, sizeof(uint64_t) * size_array);

    // timestamp in 1e-7 second
    while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
      if (offset / 64 > size_array) {
        std::cerr << "Offset too large! (" << offset << " and " << size_array << ")" << std::endl;
        exit(1);
      }

      uint64_t time = timestamp / 10000000 / 60;

      if (isWrite) { // write request
        writeTraffic_->incValue(timeInMin, length);
        writeReqs_->inc(timeInMin);
        for (uint64_t i = offset; i < offset + length; i++) {
          if (wwss[i / 64] & (((uint64_t)1) << (i % 64)) != 0) {
            updateTraffic_->inc(timeInMin);
          }
          wwss[i / 64] |= ((uint64_t)1) << (i % 64); 
        }
      } else { // read request
        readTraffic_->incValue(timeInMin, length);
        readReqs_->inc(timeInMin);
      }

      cnt++;
      if (cnt % 100000 == 0) {
        gettimeofday(&tv2, NULL);
        std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
      }
    }

    readReqs_->outputNonZero();
    writeReqs_->outputNonZero();
    readTraffic_->outputNonZero();
    writeTraffic_->outputNonZero();
    updateTraffic_->outputNonZero();
    delete wwss;
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
