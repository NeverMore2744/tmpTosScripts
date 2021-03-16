#include <iostream>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <map>
#include <unordered_map>
#include <cstdio>
#include <vector>
#include <list>
#include <set>
#include <algorithm>
#include <string>
#include <sys/time.h>
#include "large_array.h"
#include "trace.h"

#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Analyzer {
  Trace trace;

  struct Cache {
    Cache(uint64_t capacity, double ratio) {
      if (capacity == 0) capacity = 1;
      capacity_ = capacity;
      ratio_ = ratio;
    }

    std::list<uint64_t> items_;
    std::map<uint64_t, std::list<uint64_t>::iterator> map_;
    uint64_t capacity_;
    uint64_t size_ = 0;
    uint64_t numReadHits_ = 0;
    uint64_t numWriteHits_ = 0;
    double ratio_;

    void update(uint64_t addr, bool isRead) {
      if (map_.find(addr) != map_.end()) {
        size_ -= 1;
        if (isRead) {
          numReadHits_ ++;
        } else {
          numWriteHits_ ++;
        }
        items_.erase(map_[addr]);
      } else if (size_ >= capacity_) {
        size_ -= 1;
        map_.erase(items_.back());
        items_.pop_back();
      }
      size_ += 1;
      items_.push_front(addr);
      map_[addr] = items_.begin();
    }
  };

  std::vector<Cache> caches;

  uint64_t workingSetSize_ = -1ull;

  void initCaches(std::string deviceId) {
//    caches.emplace_back(workingSetSize_, 1.0);
    std::cout << caches.size() << std::endl;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace.loadProperty(propertyFileName);

    workingSetSize_ = trace.getUniqueLba(volumeId);
    caches.emplace_back(workingSetSize_ * 0.01, 0.01);
    caches.emplace_back(workingSetSize_ * 0.1, 0.1);
  }

  void analyze(char *inputTrace, uint64_t uniqueLba4k)
  {
      workingSetSize_ = uniqueLba4k;
      caches.emplace_back(workingSetSize_ * 0.01, 0.01);
      caches.emplace_back(workingSetSize_ * 0.1, 0.1);

      uint64_t offset, length, timestamp;
      char type;
      uint64_t numReadBlocks = 0, numWriteBlocks = 0;
      uint64_t numReqs = 0;
      uint64_t timeCalculated;

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

      while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
        for (uint64_t i = 0; i < length; i += 1) {
          if (type == 'R') {
            numReadBlocks += 1;
          } else {
            numWriteBlocks += 1;
          }
          uint64_t addr = offset + i;
          for (Cache &cache : caches) {
            cache.update(addr, (type == 'R'));
          }
        }

        cnt++;
        if (cnt % 100000 == 0) {
          gettimeofday(&tv2, NULL);
          std::cerr << cnt << " " << tv2.tv_sec - tv1.tv_sec << " seconds" << std::endl;
        }
      }

      std::cout << numReadBlocks << " " << numWriteBlocks << std::endl;
      for (Cache &cache : caches) {
        std::cout << cache.ratio_ << " " << cache.numReadHits_ << " " << cache.numWriteHits_ << std::endl;
      }
  }
};

int main(int argc, char *argv[]) {
  uint64_t maxLba;
  if (argc < 3) {
    std::cerr << "Input error" << std::endl;
    return 1;
  }
  sscanf(argv[2], "%llu", &maxLba);

  Analyzer analyzer;
//  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[1], maxLba);
}
