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
#include <sys/time.h>
#include "trace.h"

class Analyzer {
  Trace trace;
  uint64_t nReadReqs_ = 0, nWriteReqs_ = 0;
  uint64_t nReadBlocks_ = 0, nWriteBlocks_ = 0;
  uint64_t nUpdateBlocks_ = 0;
  uint64_t maxLba = 0;
  LargeArray<uint64_t>* writeFreq_;
  LargeArray<uint64_t>* readFreq_;

public:

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName, volume);
    writeFreq_ = new LargeArray<uint64_t>(trace.getMaxLba(volumeId) + 1);
    readFreq_ = new LargeArray<uint64_t>(trace.getMaxLba(volumeId) + 1);
  }

  void analyze(char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;

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

    while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
      if (isWrite) { // write request
        nWriteReqs_ ++;
        nWriteBlocks_ += length;
        for (uint64_t i = 0; i < length; i += 1) {
          if (writeFreq_->get(offset + i) != 0) nUpdateBlocks_ ++;
          writeFreq_->inc(offset + i);
        }
      } else { // read request
        nReadReqs_ ++;
        nReadBlocks_ += length;
        for (uint64_t i = 0; i < length; i += 1) readFreq_->inc(offset + i);
      }

      if (++cnt % 1000000 == 0) {
        gettimeofday(&tv2, NULL); std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
      }
    }

    std::cout << nReadReqs_ << " " << nReadBlocks_ << " ";
    std::cout << nWriteReqs_ << " " << nWriteBlocks_ << " ";
    std::cout << nUpdateBlocks_ << std::endl;

    readFreq_->outputNonZero();
    writeFreq_->outputNonZero();
  }
};

// params: volumeId, filename, propertyfilename
int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
