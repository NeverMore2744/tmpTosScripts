#include <stdio.h>
#include <fstream>
#include <iostream>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <vector>
#include <map>
#include "large_array.h"
#include "processor.h"

int d[10] = {1, 10, 100, 1000, 10000, 100000, 1000000};
int cp[10] = {100, 1000, 10000, 100000, 1000000, 10000000, 999999999};
int dt[10] = {1, 60, 600, 6000, 60000, 600000, 6000000, 60000000};
int cpt[10] = {600, 6000, 60000, 600000, 6000000, 60000000, 600000000, 999999999};

class Processor : Processor_base {
  FILE* fin, *fout, *fout2, *fout3, *fout4; 
  LargeArray<uint64_t> *udCount_;
  LargeArray<uint64_t> *singleTimeIn100ms_;
  LargeArray<uint64_t> *singleRwarwCount_;

  public:
    std::vector<uint64_t> pcts(std::vector<uint64_t>& times, std::vector<uint64_t>& cnts, uint64_t sum_cnts) {
      std::vector<uint64_t> rets;
      uint64_t tmp_cnt = 0;
      uint64_t ind = 1;
      assert(times.size() == cnts.size());
      for (int i = 0; i < (int)cnts.size(); i++) {
        if (ind > 100) break;
        while (ind <= 100 && tmp_cnt + cnts[i] >= (double)sum_cnts * ind / 100) {
          rets.push_back(times[i]);
          ind++;
        }
        tmp_cnt = tmp_cnt + cnts[i];
      }
      return rets;
    }

    void takeSecond(uint64_t rows, std::map<uint64_t, uint64_t>& data2cnt, uint64_t& globalSumCnt) {
      char* retChar;
      char s[200];
      uint64_t timeSecond, tmpTimes, times;
      int ret;
      uint64_t cumu = 0, zeros = 0, index = 0, lastDis = 0;
      uint64_t lastZeroIndex = 0;

      std::vector<uint64_t> timeVector;
      std::vector<uint64_t> cntVector;
      uint64_t sum_cnts = 0;
      uint64_t raw_sum_cnts = 0;

      uint64_t lastTimeSecond = -1;

      for (int outi = 0; outi < (int)rows; outi++) {
        retChar = fgets(s, 200, fin);
        if (retChar == NULL) break;
        ret = sscanf(s, "%lu %lu", &timeSecond, &tmpTimes);
        assert(ret == 2);
        `
        udCount_->incValue(timeSecond, tmpTimes);
        singleTimeIn100ms_->put(outi, timeSecond);
        singleRwarwCount_->put(outi, tmpTimes);
        raw_sum_cnts += tmpTimes;

        for (int i = lastTimeSecond + 1; i <= timeSecond; i++) {
          times = 0;
          if (i == timeSecond) times = tmpTimes;

          // fout output
          if (outi == rows - 1 && i == timeSecond) {
            fprintf(fout, "%s %d %lu\n", volumeId_.c_str(), i, cumu);
          } else if (i == cpt[index] || (i - lastDis) % dt[index] == 0) {
            if (cumu == 0) {
              if (!zeros) {
                fprintf(fout, "%s %d 0\n", volumeId_.c_str(), i);
                zeros = 1;
                lastZeroIndex = i;
              }
            } else {
              if (zeros && lastZeroIndex < i - dt[index]) {
                fprintf(fout, "%s %d 0\n", volumeId_.c_str(), i - dt[index]);
              }
              fprintf(fout, "%s %d %lu\n", volumeId_.c_str(), i, cumu);
              zeros = 0;
            }
            lastDis = i; cumu = 0;

            if (i == cpt[index]) index++;
          }
          cumu += times;

          // fout2 output: percentiles
          if (times > 0) {
            timeVector.push_back(i);
            cntVector.push_back(times);
            sum_cnts += times;
            globalSumCnt += times;
            if (data2cnt.count(i)) {
              data2cnt[i] += times;
            } else {
              data2cnt[i] = times;
            }
          }
        }

        lastTimeSecond = timeSecond;
      }

      summary();

      std::vector<uint64_t> rets = pcts(timeVector, cntVector, sum_cnts);

      int i = 1;
      for (auto& it : rets) {
        fprintf(fout2, "%s %lu %.2lf\n", volumeId_.c_str(), it, (double)i / 100);
        i++;
      }

      udCount_->outputNonZeroToFile(fout4, false);
    }

    void summary() {
      
    }

    void analyze(const char* file_prefix, const char* volume_file) {
      int index = 0;
      char filename[200], s[200];
      uint64_t cumu = 0;
      std::map<uint64_t, uint64_t> rwarw2cnt;
      uint64_t globalRwarwCnts = 0;
      char prefices[4][10] = {"rar", "war", "raw", "waw"};
      char fn_time[200], fn_time_pcts[200], fn_global_pct[200];
      char fn_gcnt[200];

      openVolumeFile(volume_file); 
      while (std::getline(is, volume)) {
        sprintf(filename, "%s/%s.data", file_prefix, volume.c_str());

        FILE* fin = fopen(filename, "r");
        if (fin == nullptr) {
          std::cerr << "Analysis of volume " << volume << " missed" << std::endl;
          continue;
        }

        std::cerr << "Processing " << volume << std::endl;
        for (int prefices_i = 0; prefices_i < 4; prefices_i++) {
          int filenames_index = filenames_.size();
          sprintf(fn_time, "%s_time.data", prefices[prefices_i]);
          sprintf(fn_time_pcts, "%s_time_pcts.data", prefices[prefices_i]);
          sprintf(fn_global_pct, "%s_global_pct.data", prefices[prefices_i]);
          sprintf(fn_gcnt, "%s_gcnt.data", prefices[prefices_i]);

          filenames_.push_back(fn_time);
          filenames_.push_back(fn_time_pcts);
          filenames_.push_back(fn_global_pct);
          filenames_.push_back(fn_gcnt);
          fout = fopen(filenames_[filenames_index].c_str(), "w");
          fout2 = fopen(filenames_[filenames_index + 1].c_str(), "w");
          fout3 = fopen(filenames_[filenames_index + 2].c_str(), "w");
          fout4 = fopen(filenames_[filenames_index + 3].c_str(), "w");

          uint64_t rows;

          // fout1: raw time
          char* retChar = fgets(s, 200, fin);
          if (retChar == nullptr) {
            std::cerr << "Empty file\n";
          }
          sscanf(s, "%lu", &rows);
          singleUdCount_ = new LargeArray<uint64_t>(rows);
          singleTimeIn100ms_ = new LargeArray<uint64_t>(rows);

          takeSecond(rows, rwarw2cnt, globalRwarwCnts);

          fclose(fout);
          fclose(fout2);
          fclose(fout3);
          fclose(fout4);
        }
        fclose(fin);
      }

      std::vector<uint64_t> retVec;
      std::vector<uint64_t> d1, d2;

      for (auto& it : rwarw2cnt) {
        d1.push_back(it.first);
        d2.push_back(it.second);
      }
      retVec = pcts(d1, d2, globalRwarwCnts);
      int j = 1;
      for (auto& it : retVec) {
        fprintf(fout1g, "%lu %.2lf\n", it, (double)j / 100);
        j++;
      }

      fclose(fout1g);

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

  return 0;
}
