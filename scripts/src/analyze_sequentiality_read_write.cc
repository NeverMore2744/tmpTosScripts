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
#include <sys/time.h>

// Use different ways of analyzers

class Analyzer {
  Trace trace;

  LargeArray<uint64_t>* indexMap_;
  std::string volumeId_;

  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;

  class ClassAnalyzer {
    private:
    int classId_;
    uint64_t maxLba_ = 0, lbaInterval_ = 32768, accessIdInterval_ = 32768;
    Analyzer* analyzer_;
    int cnts_ = 0;
    std::unordered_map<uint64_t, uint64_t> lbaCounts_;
    char filename_[200];
    FILE* fp_ = nullptr;

    std::unordered_map<uint64_t, uint64_t> readLength2num_;
    std::unordered_map<uint64_t, uint64_t> writeLength2num_;

    bool first_ = true;
    uint64_t field_ = 0;
    uint64_t lastLba_ = 0;
    char lastType_ = 0;
    uint64_t currentLength_ = 0;
    uint64_t totalCounts_ = 0;

    public:
    ClassAnalyzer(int classId, char* filePrefix): classId_(classId) {
//      sprintf(filename_, "%s.data", filePrefix);
//      std::cerr << "filename = " << filename_ << std::endl;
    }

    void analyze_class(uint64_t timestamp, char type, uint64_t offset, uint64_t length) {
      if (!first_) {
        if (offset != lastLba_ + 1 || type != lastType_) {
          if (lastType_ == 'W') {
            writeLength2num_[currentLength_]++;
          } else {
            readLength2num_[currentLength_]++;
          }
          currentLength_ = length;
        } else {
          currentLength_ += length;
        }
      } else {
        first_ = false;
        currentLength_ = length;
      }
      lastType_ = type;
      lastLba_ = offset + length - 1;
      totalCounts_ += length;
    }

    void finish_analysis() {
      if (currentLength_ > 0) {
        if (lastType_ == 'W') {
          writeLength2num_[currentLength_]++;
        } else {
          readLength2num_[currentLength_]++;
        }
      }

      for (auto& it : readLength2num_) {
        std::cout << "R " << it.first << " " << it.second << " " << totalCounts_ << "\n";
      }
      for (auto& it : writeLength2num_) {
        std::cout << "W " << it.first << " " << it.second << " " << totalCounts_ << "\n";
      }
    }
  };

public:

  void analyze(char *inputTrace, uint64_t inputMaxLba) {
      uint64_t offset, length, timestamp;
      char type;

      std::vector<ClassAnalyzer> analyzers;

      char filePrefix[200];
      strcpy(filePrefix, inputTrace);
      for (int i = 0; i < (int)strlen(inputTrace); i++) {
        if (inputTrace[i] == '.') {
          filePrefix[i] = '\0';
          break;
        }
      }

      bool finished = false;
      int openId = 0;

      std::string line;
      std::filebuf fb;
      if (!fb.open(inputTrace, std::ios::in)) {
        std::cout << "Input file error: " << inputTrace << std::endl;
        exit(1);
      }
      std::istream is(&fb);

      char line2[200];
      uint64_t cnt = 0, dis, delTime;
      timeval tv1, tv2;
      gettimeofday(&tv1, NULL);

      while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
        while ((int)analyzers.size() <= openId) {
          std::cerr << "openId = " << openId << std::endl;
          analyzers.push_back(ClassAnalyzer(analyzers.size(), filePrefix));
        }
        analyzers[openId].analyze_class(timestamp, type, offset, length);

        cnt++;
        if (cnt % 1000000 == 0) {
          gettimeofday(&tv2, NULL);
          std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
        }
      }

      for (int i = 0; i < (int)analyzers.size(); i++) {
        analyzers[i].finish_analysis();
      }

  }
};

int main(int argc, char *argv[]) {
//  Analyzer analyzer;
//  
//  if (argc < 3) {
//    printf("Error: parameter not enough\n");
//    printf("   %s <trace_file> <device_name> <futureSize> <neighborSize>\n", argv[0]);
//    return 1;
//  }
//  analyzer.init("device_property_translated.txt", argv[2]);
//  analyzer.analyze(argv[1]);
//  return 0;

  uint64_t maxLba;
  if (argc < 3) {
    std::cerr << "Input error" << std::endl;
    return 1;
  }
  sscanf(argv[2], "%llu", &maxLba);

  Analyzer analyzer;
//  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[1], maxLba);
  return 0;
}
