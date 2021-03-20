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

  void pop(uint64_t index, uint64_t udInMb) {
    uint64_t t;
    t = lifespanSqrSums->get(index);
    lifespanSqrSums->put(index, t - udInMb * udInMb);

    t = lifespanSums->get(index);
    lifespanSums->put(index, t - udInMb);

    t = lifespanNums->get(index);
    lifespanNums->put(index, t - 1);

    if (udInMb / 1024 >= 128) {
      gbBuckets[127]--;
    } else {
      gbBuckets[(int)(udInMb / 1024)]--;
    }
  }

  void summary() {
    uint64_t cnts[100];
    memset(cnts, 0, sizeof(cnts));
    uint64_t calculated = 0, coldCalculated = 0;

    for (int i = 0; i < lifespanNums->getSize(); i++) {
      uint64_t n = lifespanNums->get(i);
      if (n > 0 && n <= 4) coldCalculated++;
      else calculated++;

      if (n <= 4) continue; // ignore cold LBAs

      uint64_t sum = lifespanSums->get(i);
      uint64_t sqrSum = lifespanSqrSums->get(i);
      double avg = (double)sum / n;
      double sd = sqrt((double)(sqrSum / (n - 1)) 
          - 2.0 * avg / (n - 1) * sum + 
          (double) n / (n-1) * avg * avg);
      double cv = sd / avg;


      int index = (int)(log(cv) / log(2) * 10 + 50); 
      int index2 = (index < 0) ? 0 : (index > 100 ? 100 : (int)index);
      cnts[index2]++;

      if (i % 200000 == 3) {
        std::cerr << avg << " " << sd << " " << cv << " " << index << " " << index2<< std::endl;
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

    std::cout << calculated << " " << coldCalculated << " " << lifespanNums->getSize() << std::endl;
    std::cerr << " calculated hot " << calculated << " and cold " << coldCalculated << " size " << lifespanNums->getSize() << std::endl;
  }

} hotLbas;

class Analyzer : Analyzer_base {
  const uint32_t max_dis = (uint32_t)(1) << 29; // 128 GiB as maximum

  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lba2lbaIndex_;
//  LargeArray<uint64_t>* updateDistancesMap_;
  LargeArray<uint8_t>* frequencies_;
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

    frequencies_ = new LargeArray<uint8_t>(uniqueLba);
    coldUds_ = new LargeArray<uint32_t>(uniqueLba * 4);
    hotLbas.init(uniqueLba);
  }

  void analyze(char *inputTrace) {
    uint64_t offset, length, timestamp;
    bool isWrite;
    uint64_t timeCalculated;

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
        } else {  // a new LBA, allocate index number
          lbaIndex = maxLbaIndex;
          lba2lbaIndex_->put(lba, maxLbaIndex);
          maxLbaIndex++;
        }
        uint8_t freq = frequencies_->get(lbaIndex); // before this update, how many updates have happened
        frequencies_->put(lbaIndex, ((freq > 10) ? 11 : freq + 1));

        if (lastBlockId != 0) { // an old LBA 
          uint32_t currDisInMiB = getDistance(lba);
          hotLbas.push(lbaIndex, currDisInMiB);
          assert(freq > 0);

          if (freq < 4) { 
            coldUdIndex = lba2lbaIndex_->get(lba) * 4 + freq;
            coldUds_->put(coldUdIndex, currDisInMiB + 1);
          }
        } 

        if (freq == 4) { // fifth update, it is hot, delete everything
          coldUdIndex = lba2lbaIndex_->get(lba) * 4;
          for (uint64_t j = coldUdIndex; j <= coldUdIndex + 3; j++) {
            coldUds_->put(j, 0);
          }
        }

        indexMap_->put(lba, ++currentId_);
      }

      trace_.myTimer(false, "hotcold");
    }

    std::cerr << "finished, mapping ... processing cold LBAs ... maxLbaIndex = " << maxLbaIndex << std::endl;
    std::map<uint32_t, uint64_t> coldUd2coldUdFreq;
    uint64_t numColdLbas = 0, freq1numColdLbas = 0;

    for (uint64_t i = 0; i < coldUds_->getSize(); i++) {
      uint32_t value = coldUds_->get(i);
      if (value == 0) continue;
      coldUd2coldUdFreq[value]++;
    }

//    for (uint64_t lba = 0; lba < frequencies_->getSize(); lba++) {
//      uint8_t freq = frequencies_->get(lba);
//      if (freq == 1) freq1numColdLbas++;
//      if (freq >= 5) continue;
//      if (freq == 0) continue;
////      coldUd2coldUdFreq[getDistance(lba)]++; // This is wrong
//      numColdLbas++;
//    }
//
//    std::cerr << "finished, writing ... totally " << numColdLbas << " cold LBAs with freq 1 " << freq1numColdLbas << std::endl;
//    std::cout << coldUd2coldUdFreq.size() << std::endl;
//    for (auto& it : coldUd2coldUdFreq) {
//      std::cout << it.first << " " << it.second << std::endl;
//    }

    std::cerr << "finished, processing ... processing hot LBAs" << std::endl;
//    uint64_t cnt = 0;
//    for (uint64_t i = 0; i < coldUds_->getSize(); i++) {
//      uint32_t ud = coldUds_->get(i);
//      if (ud == 0) continue;
//      hotLbas.pop(i / 4, ud);
//      cnt++;
//    }
//    std::cerr << "removed " << cnt << " lifespans on cold LBAs" << std::endl;

    hotLbas.summary();

  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
  return 0;
}
