#include "large_array.h"
#include "trace.h"

class Analyzer : Analyzer_base {
  // LBA to number of reads and number of writes
  std::set<uint64_t> lbas_;
  uint64_t maxLba = 0;

public:
  void init(char *volume) {
    std::string volumeId(volume);
    trace_.loadProperty(nullptr, volume);
  }

  void analyze(char *inputTrace, char *volumeId)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;
    openTrace(inputTrace);
    trace_.myTimer(true, "property");

    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      maxLba = std::max(maxLba, offset + length);
      for (uint64_t i = 0; i < length; i += 1) {
        lbas_.insert(offset + i);
      }
      trace_.myTimer(false, "property");
    }
    std::cout << volumeId << " " << lbas_.size() << " " << maxLba << std::endl;
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  analyzer.init(argv[1]);
  analyzer.analyze(argv[2], argv[1]);
}
