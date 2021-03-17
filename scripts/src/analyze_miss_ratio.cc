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

class Analyzer {
  Trace trace;
  std::string volumeId_;

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
    std::cerr << caches.size() << std::endl;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    volumeId_ = volumeId;
    trace.loadProperty(propertyFileName, volume);

    workingSetSize_ = trace.getUniqueLba(volumeId);
    caches.emplace_back(workingSetSize_ * 0.01, 0.01);
    caches.emplace_back(workingSetSize_ * 0.1, 0.1);
  }

  void analyze(char *inputTrace)
  {
      caches.emplace_back(workingSetSize_ * 0.01, 0.01);
      caches.emplace_back(workingSetSize_ * 0.1, 0.1);

      uint64_t offset, length, timestamp;
      bool isWrite;
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
      trace.myTimer(true, "miss ratio");

      while (trace.readNextRequestFstream(is, timestamp, isWrite, offset, length, line2)) {
        for (uint64_t i = 0; i < length; i += 1) {
          if (!isWrite) {
            numReadBlocks += 1;
          } else {
            numWriteBlocks += 1;
          }
          uint64_t addr = offset + i;
          for (Cache &cache : caches) {
            cache.update(addr, !isWrite);
          }
        }

        trace.myTimer(false, "miss ratio");
      }

      for (Cache &cache : caches) {
        std::cout << volumeId_ << " " << numReadBlocks << " " << numWriteBlocks << " "  
          << cache.ratio_ << " " << cache.numReadHits_ << " " << cache.numWriteHits_ << std::endl;
      }
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
