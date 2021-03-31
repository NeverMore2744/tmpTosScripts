#include "large_array.h"
#include "trace.h"

class Analyzer : Analyzer_base {
  LargeArray<uint64_t>* indexMap_;
  LargeArray<uint64_t>* lastTimestamp_;
  LargeArray<char>* lastState_;

  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
//    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } waw_;
  struct {
    LargeArray<uint64_t>* intervalHistogramByTime_;
//    LargeArray<uint64_t>* intervalHistogramByDataAmount_;
  } raw_;

  uint64_t currentId_ = 0;

  uint64_t getDistance(uint64_t off) {
    uint64_t distance = currentId_ - indexMap_->get(off);
    distance /= 256;
    if (distance >= nBlocks_ * 8 / 256) distance = nBlocks_ * 8 / 256;
    return distance;
  }

  uint64_t getTime(uint64_t off, uint64_t timestamp) {
    if (timestamp < lastTimestamp_->get(off)) {
      return -1llu;
    }
    uint64_t diff = timestamp - lastTimestamp_->get(off);
    diff /= 10000000;
    return diff;
  }

public:

  // initialize properties
  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace_.loadProperty(propertyFileName, volume);

    uint64_t maxLba = trace_.getMaxLba(volumeId);
    nBlocks_ = maxLba + 1;
    std::cerr << nBlocks_ << std::endl;

    indexMap_ = new LargeArray<uint64_t>(nBlocks_);
    lastTimestamp_ = new LargeArray<uint64_t>(nBlocks_);
    lastState_ = new LargeArray<char>(nBlocks_);

    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    waw_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 3600);
//    waw_.intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);

    // every 256 blocks in one bucket, in total maintain (maxLba / 4096 + 1) * 8
    raw_.intervalHistogramByTime_ = new LargeArray<uint64_t>(31 * 24 * 3600);
//    raw_.intervalHistogramByDataAmount_ = new LargeArray<uint64_t>(nBlocks_ * 8 / 256 + 1);
  }

  void analyze(char *inputTrace) {
      uint64_t offset, length, timestamp;
      bool isWrite;
      openTrace(inputTrace);
      bool first = true;
      uint64_t lastReqTimestamp;
      trace_.myTimer(true, "RAW and WAW");

      while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
        if (first) {
          first = false;
        } else if (timestamp < lastReqTimestamp) {
          continue;
        }
        lastReqTimestamp = timestamp;

        if (!isWrite) { // read request
          for (uint64_t i = 0; i < length; i += 1) {
            uint8_t last = lastState_->get(offset + i);
            if (last == 'W') {
//              raw_.intervalHistogramByDataAmount_->inc(getDistance(offset + i));
              raw_.intervalHistogramByTime_->inc(getTime(offset + i, timestamp));
            }

            lastState_->put(offset + i, 'R');
            lastTimestamp_->put(offset + i, timestamp);
          }
        } else { // write request
          for (uint64_t i = 0; i < length; i += 1) {
            uint8_t last = lastState_->get(offset + i);
            if (last == 'W') {
//              waw_.intervalHistogramByDataAmount_->inc(getDistance(offset + i));
              waw_.intervalHistogramByTime_->inc(getTime(offset + i, timestamp));
            }

            lastState_->put(offset + i, 'W');
            lastTimestamp_->put(offset + i, timestamp);
            indexMap_->put(offset + i, currentId_++);
          }
        }

        trace_.myTimer(false, "RAW and WAW");
      }

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
  analyzer.init(argv[3], argv[2]);
  analyzer.analyze(argv[1]);
}
