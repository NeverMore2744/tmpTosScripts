#include <unordered_set>
#include "large_array.h"
#include "trace.h"

class Analyzer : Analyzer_base {
  std::unordered_map<std::string, double> ratios;

public:
  void init(char* propertyFile, char *volume) {
    std::string volumeId(volume);
    volumeId_ = std::string(volume);
    trace_.loadProperty(propertyFile, volume);
  }

  void loadRatios(const char *inputFile) {
    FILE *f = nullptr; 
    double ell_ratio, tp;
    std::string volumeId;

    if (inputFile != nullptr) {
      f = fopen(inputFile, "r");
      if (f == nullptr) {
        std::cerr << "not exist " << inputFile << std::endl;
        exit(1);
      }
      char tmp[300];
      while (fscanf(f, "%s %lf %lf", tmp, &ell_ratio, &tp) != EOF) {
        volumeId = std::string(tmp);
        ratios[volumeId] = ell_ratio;
      }
    }
    fclose(f);
  }

  void analyze(const char *inputTrace)
  {
    uint64_t offset, length, timestamp;
    bool isWrite;
    openTrace(inputTrace);
    trace_.myTimer(true, "property");

    uint64_t trafficWindowSize = trace_.getUniqueLba(volumeId_) * ratios[volumeId_];

    std::cerr << volumeId_ << " " << trafficWindowSize << " " << trace_.getUniqueLba(volumeId_) 
      << " " << ratios[volumeId_] << std::endl;
    std::unordered_set<uint64_t> lbas;
    uint64_t trafficCnt = 0;

    uint64_t totNumUniqueLbas = 0, totTimes = 0, finalNumUniqueLbas = 0;

    while (trace_.readNextRequestFstream(*is_, timestamp, isWrite, offset, length, line2_)) {
      if (!isWrite) continue;

      for (int i = 0; i < length; i++) {
        if (trafficCnt >= trafficWindowSize) {
          totNumUniqueLbas += lbas.size();
          finalNumUniqueLbas = lbas.size();
          totTimes ++;
          lbas.clear();
          trafficCnt = 0;
        }

        trafficCnt++;
        lbas.insert(offset + i);
      }

      trace_.myTimer(false, "property");
    }
    std::cout << volumeId_ << " " << std::fixed << trace_.getUniqueLba(volumeId_) 
      << " " << (double)totNumUniqueLbas / totTimes << " " << finalNumUniqueLbas << std::endl;
  }
};

int main(int argc, char *argv[]) {
  if (argc < 4) {
    exit(1);
  }

  Analyzer analyzer;
  analyzer.init(argv[3], argv[1]);
  analyzer.loadRatios(argv[4]);
  analyzer.analyze(argv[2]);
}
