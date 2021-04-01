#include "large_array.h"
#include "trace.h"

class Analyzer : Analyzer_base {
  // LBA to number of reads and number of writes
  uint64_t maxLba = 0;
  LargeArray<uint8_t> *lbas_;
  uint64_t arrSize = 100000;

  void resize(uint64_t lba) {
    LargeArray<uint8_t> *newArray;
    std::cerr << "Resize volume " << volumeId_ << " LBA array size from " << arrSize << " to ";
    while (lba >= arrSize) {
      arrSize *= 2; 
    }
    newArray = new LargeArray<uint8_t>(arrSize);
    
    // move
    for (int i = 0; i < lbas_->getSize(); i++) {
      if (lbas_->get(i) > 0) {
        newArray->put(i, 1);
      }
    }

    std::cerr << arrSize << std::endl;
    delete lbas_; 
    lbas_ = newArray;
  }

public:
  void init(char *volume) {
    std::string volumeId(volume);
    volumeId_ = volumeId;
    trace_.loadProperty(nullptr, volume);
  }

  void analyze(char *inputTrace, char *volumeId)
  {
    uint64_t offset, length, timestamp, wss = 0;
    bool isWrite;
    openTrace(inputTrace);
    trace_.myTimer(true, "property");

    lbas_ = new LargeArray<uint8_t>(arrSize);

    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      maxLba = std::max(maxLba, offset + length);
      for (uint64_t i = 0; i < length; i += 1) {
        if (offset + i >= arrSize) {
          resize(offset + i);
        }
        lbas_->put(offset + i, 1);
      }
      trace_.myTimer(false, "property");
    }

    for (int i = 0; i < lbas_->getSize(); i++) {
      if (lbas_->get(i) > 0) wss++;
    }
    std::cout << volumeId << " " << wss << " " << maxLba << std::endl;
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[1]);
  analyzer.analyze(argv[2], argv[1]);
}
