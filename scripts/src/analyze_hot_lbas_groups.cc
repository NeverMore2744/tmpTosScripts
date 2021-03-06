#include <cmath>
#include <unordered_set>
#include "large_array.h"
#include "trace.h"

#include <cassert>

struct lbaStat {
  LargeArray<uint64_t>* lifespanNums, *lifespanSums, *lifespanSqrSums;
  uint64_t gbBuckets[128];

  void init(uint64_t length) {
    memset(gbBuckets, 0, sizeof(gbBuckets));
    lifespanNums = new LargeArray<uint64_t>(length);
    lifespanSums = new LargeArray<uint64_t>(length);
    lifespanSqrSums = new LargeArray<uint64_t>(length);
  }

  void push(uint64_t index, uint64_t udInMb) {
    uint64_t t;
    t = lifespanSqrSums->get(index);
    lifespanSqrSums->put(index, t + udInMb * udInMb);

    t = lifespanSums->get(index);
    lifespanSums->put(index, t + udInMb);

    t = lifespanNums->get(index);
    lifespanNums->put(index, t + 1);

    if (udInMb / 1024 >= 128) {
      gbBuckets[127]++;
    } else {
      gbBuckets[(int)(udInMb / 1024)]++;
    }
  }

  struct MyStruct {
    uint64_t index, value;

    bool operator < (const MyStruct& str) const {
      return (value > str.value);
    }
  };

  void summary() {
    uint64_t cnts[100], len = lifespanNums->getSize(), i;
    memset(cnts, 0, sizeof(cnts));
    uint64_t calculated = 0, coldCalculated = 0;

    std::cerr << len << std::endl;

    struct MyStruct* sortor = new MyStruct[len];
    for (int i = 0; i < len; i++) {
      sortor[i].index = i;
      sortor[i].value = lifespanNums->get(i);
    }

    std::sort(sortor, sortor + len); 
    for (int i = 0; i < 10; i++) {
      std::cerr << i << " " << sortor[i].index << " " << sortor[i].value << "\n";
    }

    for (int outi = 0; outi < len; outi++) {
      i = sortor[outi].index;

      uint64_t n = lifespanNums->get(i);
      if (n > 1) { 
        uint64_t sum = lifespanSums->get(i);
        uint64_t sqrSum = lifespanSqrSums->get(i);
        double avg = (double)sum / n;
        double sd = sqrt((double)sqrSum / (n - 1) 
            - 2.0 * avg / (n - 1) * sum + 
            (double) n / (n-1) * avg * avg);
        double cv = (sum == 0) ? 0.0 : sd / avg;

        int index = (cv == 0.0) ? 0 : (int)(log(cv) / log(2) * 25 + 50); 
        int index2 = (index < 0) ? 0 : (index > 100 ? 100 : (int)index);
        cnts[index2]++;

        if (outi % 200000 == 3) {
          std::cerr << std::fixed << n << " " << sum << " " << sqrSum << " " << avg << " " << sd << " " << cv << " " << index << " " << index2 << std::endl;
        }
      }

      if (outi % (len / 1000) == 0) {
        std::cout << outi << " " << n << std::endl;
        std::cout << 100 << std::endl;;
        for (int i = 0; i < 100; i++) {
          std::cout << std::fixed << i << " " << cnts[i] << std::endl;
        }
        std::cout << 128 << std::endl;;
        for (int i = 0; i < 128; i++) {
          std::cout << std::fixed << i << " " << gbBuckets[i] << std::endl;
        }
      }
    }

    std::cout << 100 << std::endl;;
    for (int i = 0; i < 100; i++) {
      std::cout << i << " " << cnts[i] << std::endl;
    }
    std::cout << 128 << std::endl;;
    for (int i = 0; i < 128; i++) {
      std::cout << i << " " << gbBuckets[i] << std::endl;
    }
  }

} hotLbas;

class Analyzer : Analyzer_base {
  const uint32_t max_dis = (uint32_t)(1) << 29; // 128 GiB as maximum

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lba2lbaIndex_;
//  LargeArray<uint64_t>* updateDistancesMap_;
  LargeArray<uint32_t>* coldUds_;

  uint64_t startTimestamp_ = 0, currentId_ = 0;
  uint64_t maxLba;
  uint64_t uniqueLba;

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
    trace_.loadProperty(propertyFileName, volume);

    maxLba = trace_.getMaxLba(volumeId);
    uniqueLba = trace_.getUniqueLba(volumeId);
    nBlocks_ = maxLba + 1;

    std::cerr << "nBlocks = " << nBlocks_ << std::endl;
    std::cerr << "WsS = " << uniqueLba << std::endl;

    lba2lbaIndex_ = new LargeArray<uint64_t>(nBlocks_);
    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    hotLbas.init(uniqueLba);
  }

  void analyze(char *inputTrace) {
    uint64_t offset, length, timestamp;
    bool isWrite;

    openTrace(inputTrace);
    uint64_t maxLbaIndex = 0, lbaIndex = 0, lba, coldUdIndex;

    trace_.myTimer(true, "hotcold");

    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      if (!isWrite) continue;

      for (uint64_t i = 0; i < length; i += 1) {
        lba = offset + i;
        uint64_t lastBlockId = indexMap_->get(lba);

        if (lastBlockId != 0) { // an old LBA
          lbaIndex = lba2lbaIndex_->get(lba);
          uint32_t currDisInMiB = getDistance(lba);
          hotLbas.push(lbaIndex, currDisInMiB);
        } else {  // a new LBA, allocate index number
          lbaIndex = maxLbaIndex;
          lba2lbaIndex_->put(lba, maxLbaIndex);
          maxLbaIndex++;
        }

        indexMap_->put(lba, ++currentId_);
      }

      trace_.myTimer(false, "hotcold");
    }

    std::cerr << "finished, maxLbaIndex = " << maxLbaIndex << std::endl;
    hotLbas.summary();

  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
  return 0;
}
