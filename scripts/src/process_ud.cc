#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <vector>
#include "processor.h"

// MSR: use minute
// Ali: use second
// ??????

class Processor : Processor_base {
  public:
    void analyze(const char* file_prefix, const char* volume_file) {
      filenames_.push_back("ud_time.data");
      filenames_.push_back("ud_amount.data");
      FILE* fout = fopen(filenames_[0].c_str(), "w");
      FILE* fout2 = fopen(filenames_[1].c_str(), "w");
      fprintf(fout, "log timeInSec cnts\n"); 
      fprintf(fout2, "log amountInMb cnts\n");

      int d[10] = {1, 10, 100, 1000, 10000, 100000, 1000000};
      int cp[10] = {100, 1000, 10000, 100000, 1000000, 10000000, 999999999};

      // For msr
      int dt[10] = {1, 60, 600, 6000, 60000};
      int cpt[10] = {6000, 60000, 600000, 6000000, 60000000};
      int index = 0;
      uint64_t cumu = 0, addr;
      std::vector<uint64_t> ud_counts;
      openVolumeFile(volume_file);

      while (std::getline(*is_, volumeId_)) {
        sprintf(filename, "%s/%s.data", file_prefix, volumeId_.c_str());
        fin = fopen(filename, "r");

        if (fin == nullptr) {
          std::cerr << "Analysis of volume " << volumeId_ << " missed" << std::endl;
          continue;
        }

        uint64_t times;
        char* retChar;
        int ret;

        //    const int intv = 60;
        int lastZeroIndex;
        uint64_t rows;
        uint64_t lastDis = 0;
        uint64_t lastTimeSecond = -1, timeSecond = 0, tmpTimes = 0;

        int zeros = 0;
        cumu = 0;
        fgets(s, 200, fin);
        sscanf(s, "%lld", &rows);
        printf("rows = %llu\n", rows);

        index = 0; lastDis = 0;
        for (int outi = 0; outi < (int)rows; outi++) {
          retChar = fgets(s, 200, fin);
          if (retChar == NULL) break;
          ret = sscanf(s, "%lld%lld", &timeSecond, &tmpTimes);
          assert(ret == 2);

          for (int i = lastTimeSecond + 1; i <= timeSecond; i++) {
            times = 0;
            if (i == timeSecond) times = tmpTimes;

            if (outi == rows - 1 && i == timeSecond) {
              fprintf(fout, "%s %d %lld\n", logname, i, cumu);
            } else if (i == cpt[index] || (i - lastDis) % dt[index] == 0) {
              if (cumu == 0) {
                if (!zeros) {
                  fprintf(fout, "%s %d 0\n", logname, i);
                  zeros = 1;
                  lastZeroIndex = i;
                }
              } else {
                if (zeros && lastZeroIndex < i - dt[index]) {
                  fprintf(fout, "%s %d 0\n", logname, i - dt[index]);
                }
                fprintf(fout, "%s %d %lld\n", logname, i, cumu);
                zeros = 0;
              }
              lastDis = i; cumu = 0;

              if (i == cpt[index]) index++;
            }
            cumu += times;
          }

          lastTimeSecond = timeSecond;
        }

        retChar = fgets(s, 200, fin);
        assert(retChar);
        sscanf(s, "%lld", &rows);
        printf("rows = %lld\n", rows);
        lastTimeSecond = -1;

        index = 0;
        cumu = 0;
        zeros = 0;
        for (int outi = 0; outi < rows; outi++) {
          retChar = fgets(s, 200, fin);
          if (retChar == NULL) break;
          ret = sscanf(s, "%lld%lld", &timeSecond, &tmpTimes);
          assert(ret == 2);

          for (int i = lastTimeSecond + 1; i <= timeSecond; i++) {
            times = 0;
            if (i == timeSecond) times = tmpTimes;

            if (outi == rows - 1 && i == timeSecond) {
              fprintf(fout2, "%s %d %lld\n", logname, i, cumu);
            } else if (i == cp[index] || (i - lastDis) % d[index] == 0) {
              if (cumu == 0) {
                if (!zeros) {
                  fprintf(fout2, "%s %d 0\n", logname, i);
                  zeros = 1;
                  lastZeroIndex = i;
                }
              } else {
                if (zeros && lastZeroIndex < i - d[index]) {
                  fprintf(fout2, "%s %d 0\n", logname, i - d[index]);
                }
                fprintf(fout2, "%s %d %lld\n", logname, i, cumu);
                zeros = 0;
              }
              lastDis = i; cumu = 0;

              if (i == cp[index]) index++;
            }
            cumu += times;
          }

          lastTimeSecond = timeSecond;
        }

        fclose(fin);
      }

      fclose(fout);
      fclose(fout2);
      outputFileNames();
    }
};

int main(int argc, char** argv) {
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  if (argc < 3) {
    std::cerr << "Error - params not enough. " << argv[0] << " <file_prefix> <volume_file>\n";
    exit(1);
  }

  Processor processor;
  processor.analyze(argv[1], argv[2]);
}
