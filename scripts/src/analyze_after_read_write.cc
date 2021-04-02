#include "large_array.h"
#include "trace.h"

class Analyzer : Analyzer_base {
  LargeArray<uint64_t>* lba2index_;
  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lastState_;

  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
//    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } rar_, war_, raw_, waw_;

  uint64_t currentId_ = 0;

  uint64_t getDistance(uint64_t off) {
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 256;
    if (distance >= nBlocks_ * 8 / 256) distance = nBlocks_ * 8 / 256;
    return distance;
  }

  uint64_t getTimeWithLastTime(uint64_t index, uint64_t timestamp, uint64_t lastTimestamp) {
    if (timestamp < lastTimestamp) {
      return -1llu;
    }
    return (timestamp - lastTimestamp) / 1000000; // Unit: 0.1 s
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace_.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace_.getMaxLba(volumeId);
    uint64_t wss = trace_.getUniqueLba(volumeId);

    nBlocks_ = maxLba + 1;
    std::cerr << nBlocks_ << std::endl;

    lba2index_ = new LargeArray<uint64_t>(maxLba + 1);
    indexMap_ = new LargeArray<uint64_t>(wss + 10);
    lastState_ = new LargeArray<uint64_t>(wss + 10);

    rar_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 36000);
    war_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 36000);

    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    waw_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 36000);
//    waw_.intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);

    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    raw_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 36000);
//    raw_.intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);

  }

  void analyze(char *inputTrace) {
      uint64_t offset, length, timestamp;
      bool isWrite;
      openTrace(inputTrace);

      bool first = true;
      uint64_t lastReqTimestamp;
      trace_.myTimer(true, "RAR, WAR, RAW, and WAW");

      uint64_t wssPtr = 1, lba, index;

      while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
        if (first) {
          first = false;
        } else if (timestamp < lastReqTimestamp) {
          continue;
        }
        lastReqTimestamp = timestamp;

        if (!isWrite) { // read request
          for (uint64_t i = 0; i < length; i += 1) {
            lba = offset + i;
            index = lba2index_->get(lba);
            if (index == 0) { // First request
              index = wssPtr++;
              lba2index_->put(lba, index);
            }

            uint64_t last = lastState_->get(index);
            if (last % 256 == 'W') {
//              raw_.intervalHistogramByDataAmount_->inc(getDistance(offset + i));
              raw_.intervalHistogramByTime_->inc(getTimeWithLastTime(index, timestamp, last / 256));
            } else if (last % 256 == 'R') {
              rar_.intervalHistogramByTime_->inc(getTimeWithLastTime(index, timestamp, last / 256));
            }

            lastState_->put(index, (timestamp * 256) + 'R');
          }
        } else { // write request
          for (uint64_t i = 0; i < length; i += 1) {
            lba = offset + i;
            index = lba2index_->get(lba);
            if (index == 0) { // First request
              index = wssPtr++;
              lba2index_->put(lba, index);
            }

            uint64_t last = lastState_->get(index);
            if (last % 256 == 'W') {
//              waw_.intervalHistogramByDataAmount_->inc(getDistance(offset + i));
              waw_.intervalHistogramByTime_->inc(getTimeWithLastTime(index, timestamp, last / 256));
            } else if (last % 256 == 'R') {
              war_.intervalHistogramByTime_->inc(getTimeWithLastTime(index, timestamp, last / 256));
            }

            lastState_->put(index, (timestamp * 256) + 'W');
            indexMap_->put(index, currentId_++);
          }
        }

        trace_.myTimer(false, "RAR, WAR, RAW, and WAW");
      }

      std::cerr << "Final: wssPtr = " << wssPtr << ", size = " << indexMap_->getSize() << std::endl;
      std::cerr << "Output: rar time" << std::endl;
      rar_.intervalHistogramByTime_->outputNonZero();
      std::cerr << "Output: war time" << std::endl;
      war_.intervalHistogramByTime_->outputNonZero();
      std::cerr << "Output: raw time" << std::endl;
      raw_.intervalHistogramByTime_->outputNonZero();
//      std::cerr << "Output: raw data amount" << std::endl;
//      raw_.intervalHistogramByDataAmount_->outputNonZero();
      std::cerr << "Output: waw time" << std::endl;
      waw_.intervalHistogramByTime_->outputNonZero();
//      std::cerr << "Output: waw data amount" << std::endl;
//      waw_.intervalHistogramByDataAmount_->outputNonZero();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
