#include "large_array.h"
#include "trace.h"

class Analyzer : Analyzer_base {
  // LBA to number of reads and number of writes
  LargeArray<uint64_t> *readTraffic_;
  LargeArray<uint64_t> *writeTraffic_;
  LargeArray<uint64_t> *totTraffic_;
  uint64_t maxLba_;

public:
  Analyzer() {
    readTraffic_ = new LargeArray<uint64_t>(24*60);
    writeTraffic_ = new LargeArray<uint64_t>(24*60);
    totTraffic_ = new LargeArray<uint64_t>(24*60);
  }

  void init(char *propertyFileName, char *volume) {
    std::string volumeId(volume);
    trace_.loadProperty(propertyFileName, volume);
    maxLba_ = trace_.getMaxLba(volumeId);
  }

  void analyze(char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;

    openTrace(inputTrace);
    trace_.myTimer(true, "mid2mid");

    int cnt = 0;

    // timestamp in 1e-7 second
    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      uint64_t timeInMin = timestamp / 10000000 / 60;
      uint64_t timeInMinAlignedZone = (timeInMin + trace_.timeZoneOffset_ * 60) % (24 * 60);
      
      if (isWrite) { // write request
        writeTraffic_->incValue(timeInMinAlignedZone, length);
      } else { // read request
        readTraffic_->incValue(timeInMinAlignedZone, length);
      }
      totTraffic_->incValue(timeInMinAlignedZone, length);

      trace_.myTimer(false, "mid2mid");
    }

    readTraffic_->outputNonZero();
    writeTraffic_->outputNonZero();
    totTraffic_->outputNonZero();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.analyze(argv[2]);
}
