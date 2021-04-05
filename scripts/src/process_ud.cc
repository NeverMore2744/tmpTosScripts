#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <vector>
#include "processor.h"
#include "large_array.h"

// MSR: use minute
// Ali: use second
// ??????

class Processor : Processor_base {
  LargeArray<uint64_t> *udCount_;
  LargeArray<uint64_t> *singleTimeIn100ms_;
  LargeArray<uint64_t> *singleUdCount_;
  uint64_t sumUdCnts_;

  void summary(FILE* fout3, FILE* fout4) {
    uint64_t pcts[5] = {25, 50, 75, 90, 95};
    uint64_t chkpt[4] = {2999, 17999, 144000, 26783999};
    int index3 = 0, index4 = 0;
    uint64_t tmpSumCnts = 0, sumCnts4 = 0, sumCnts4Printed = 0;
    if (sumUdCnts_ == 0) {
      return;
    }

    for (int i = 0; i < singleUdCount_->getSize(); i++) {
      tmpSumCnts += singleUdCount_->get(i);

      while (index3 <= 4 && tmpSumCnts >= sumUdCnts_ * pcts[index3] / 100) { // Percentiles
        fprintf(fout3, "%luth %.1lf\n", pcts[index3], (double)singleTimeIn100ms_->get(i) / 10);
        index3++;
      }

      while (index4 <= 3 && singleTimeIn100ms_->get(i) > chkpt[index4]) { // Percentages
        fprintf(fout4, "%d %.5lf\n", index4 + 1, (double)sumCnts4 / sumUdCnts_);
        sumCnts4Printed += sumCnts4;
        sumCnts4 = 0;
        if (index4 == 2) {
          fprintf(fout4, "%d %.5lf\n", index4 + 2, 1.0 - (double)sumCnts4Printed / sumUdCnts_);
          index4++;
        }
        index4++;
      }

      sumCnts4 += singleUdCount_->get(i);
      if (index3 > 4 && index4 > 3) break;
    }
  }

  public:
    void analyze(const char* file_prefix, const char* volume_file) {
      filenames_.push_back("ud_time.data");
      filenames_.push_back("total_ud_time.data");
      filenames_.push_back("levels.data");
      filenames_.push_back("groups.data");
      udCount_ = new LargeArray<uint64_t>(31 * 24 * 36000);

      FILE *fin, *fout = fopen(filenames_[0].c_str(), "w");
      FILE *fout2 = fopen(filenames_[1].c_str(), "w");
      FILE *fout3 = fopen(filenames_[2].c_str(), "w");
      FILE *fout4 = fopen(filenames_[3].c_str(), "w");

      fprintf(fout, "log timeInSec cnts\n"); 
      fprintf(fout2, "timeIn100ms cnts\n");
      fprintf(fout3, "type ud\n");
      fprintf(fout4, "type pcts\n");

      int d[10] = {1, 10, 100, 1000, 10000, 100000, 1000000};
      int cp[10] = {100, 1000, 10000, 100000, 1000000, 10000000, 999999999};

      // For msr
      int delta_time[10] = {1, 60, 600, 6000, 60000};
      int chkpt_time[10] = {6000, 60000, 600000, 6000000, 60000000};
      int index = 0;
      uint64_t cumu = 0, addr;
      openVolumeFile(volume_file);
      char filename[200], s[200];

      while (std::getline(*is_, volumeId_)) {
        sprintf(filename, "%s/%s.data", file_prefix, volumeId_.c_str());
        fin = fopen(filename, "r");

        if (fin == nullptr) {
          std::cerr << "Analysis of volume " << volumeId_ << " missed" << std::endl;
          continue;
        }

        sumUdCnts_ = 0;

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
        fprintf(stderr, "Volume %s rows = %llu\n", volumeId_.c_str(), rows);

        singleUdCount_ = new LargeArray<uint64_t>(rows);
        singleTimeIn100ms_ = new LargeArray<uint64_t>(rows);

        index = 0; lastDis = 0;
        for (int outi = 0; outi < (int)rows; outi++) {
          retChar = fgets(s, 200, fin);
          if (retChar == NULL) break;

          ret = sscanf(s, "%lld%lld", &timeSecond, &tmpTimes);
          assert(ret == 2);

          udCount_->incValue(timeSecond, tmpTimes);

          singleTimeIn100ms_->put(outi, timeSecond);
          singleUdCount_->put(outi, tmpTimes);
          sumUdCnts_ += tmpTimes;

          for (int i = lastTimeSecond + 1; i <= timeSecond; i++) {
            times = 0;
            if (i == timeSecond) times = tmpTimes;
            cumu += times;

            if (outi == rows - 1 && i == timeSecond) {  // Have to output because it comes to the end
              fprintf(fout, "%s %.1f %lld\n", volumeId_.c_str(), (double)i / 10, cumu);
            } else if (i == chkpt_time[index] || (i - lastDis) % delta_time[index] == 0) {  // Goes to the threshold, or goes to the interval
              if (cumu == 0) {
                if (!zeros) { // The zero does not starts
                  fprintf(fout, "%s %.1f 0\n", volumeId_.c_str(), (double)i / 10);
                  zeros = 1;
                  lastZeroIndex = i;
                }
              } else {
                if (zeros && lastZeroIndex < i - delta_time[index]) {
                  fprintf(fout, "%s %.1f 0\n", volumeId_.c_str(), (double)(i - delta_time[index]) / 10);
                }
                fprintf(fout, "%s %.1f %lld\n", volumeId_.c_str(), (double)i / 10, cumu);
                zeros = 0;
              }
              lastDis = i; cumu = 0;

              if (i == chkpt_time[index]) index++;
            }
//            cumu += times;
          }

          lastTimeSecond = timeSecond;
        }

        /*
        retChar = fgets(s, 200, fin);
        assert(retChar);
        sscanf(s, "%lld", &rows);
        fprintf(stderr, "rows = %lld\n", rows);
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
              fprintf(fout2, "%s %d %lld\n", volumeId_.c_str(), i, cumu);
            } else if (i == cp[index] || (i - lastDis) % d[index] == 0) {
              if (cumu == 0) {
                if (!zeros) {
                  fprintf(fout2, "%s %d 0\n", volumeId_.c_str(), i);
                  zeros = 1;
                  lastZeroIndex = i;
                }
              } else {
                if (zeros && lastZeroIndex < i - d[index]) {
                  fprintf(fout2, "%s %d 0\n", volumeId_.c_str(), i - d[index]);
                }
                fprintf(fout2, "%s %d %lld\n", volumeId_.c_str(), i, cumu);
                zeros = 0;
              }
              lastDis = i; cumu = 0;

              if (i == cp[index]) index++;
            }
            cumu += times;
          }

          lastTimeSecond = timeSecond;
        }
        */

        fclose(fin);
        summary(fout3, fout4);
        delete singleTimeIn100ms_;
        delete singleUdCount_;
      }

      udCount_->outputNonZeroToFile(fout2, false);
      fclose(fout);
      fclose(fout2);
      fclose(fout3);
      fclose(fout4);

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
