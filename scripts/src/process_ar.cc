#include <stdio.h>
#include <fstream>
#include <iostream>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <vector>
#include <map>

int d[10] = {1, 10, 100, 1000, 10000, 100000, 1000000};
int cp[10] = {100, 1000, 10000, 100000, 1000000, 10000000, 999999999};
int dt[10] = {1, 60, 600, 6000, 60000, 600000, 6000000, 60000000};
int cpt[10] = {600, 6000, 60000, 600000, 6000000, 60000000, 600000000, 999999999};

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

void takeSecond(FILE* fin, FILE* fout, FILE* fout2, const char* logname, uint64_t rows, std::map<uint64_t, uint64_t>& data2cnt, uint64_t& globalSumCnt) {
  char* retChar;
  char s[200];
  uint64_t timeSecond, tmpTimes, times;
  int ret;
  uint64_t cumu = 0, zeros = 0, index = 0, lastDis = 0;
  uint64_t lastZeroIndex = 0;

  std::vector<uint64_t> timeVector;
  std::vector<uint64_t> cntVector;
  uint64_t sum_cnts = 0;

  uint64_t lastTimeSecond = -1;

  for (int outi = 0; outi < (int)rows; outi++) {
    retChar = fgets(s, 200, fin);
    if (retChar == NULL) break;
    ret = sscanf(s, "%lu %lu", &timeSecond, &tmpTimes);
    assert(ret == 2);

    for (int i = lastTimeSecond + 1; i <= timeSecond; i++) {
      times = 0;
      if (i == timeSecond) times = tmpTimes;

      // fout output
      if (outi == rows - 1 && i == timeSecond) {
        fprintf(fout, "%s %d %lu\n", logname, i, cumu);
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
          fprintf(fout, "%s %d %lu\n", logname, i, cumu);
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

  std::vector<uint64_t> rets = pcts(timeVector, cntVector, sum_cnts);

  int i = 1;
  for (auto& it : rets) {
    fprintf(fout2, "%s %lu %.2lf\n", logname, it, (double)i / 100);
    i++;
  }
}

int main(int argc, char** argv) {
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  if (argc < 3) {
    std::cerr << "Error - params not enough. " << argv[0] << " <file_prefix> <volume_file>\n";
    exit(1);
  }

  FILE* fout = fopen("rar_time.data", "w");
  FILE* fout3 = fopen("war_time.data", "w");
  int num_logs = 0;

  fprintf(fout, "log timeInSec cnts\n"); 
  fprintf(fout3, "log timeInSec cnts\n");
  
  FILE* fout1_2 = fopen("rar_time_pcts.data", "w");
  FILE* fout3_2 = fopen("war_time_pcts.data", "w");
  fprintf(fout1_2, "log timeInSec pct\n"); 
  fprintf(fout3_2, "log timeInSec pct\n");

  int index = 0;
  char filename[200], s[200];
  uint64_t cumu = 0;
  std::map<uint64_t, uint64_t> war2cnt;
  std::map<uint64_t, uint64_t> rar2cnt;
  uint64_t globalWarCnts = 0, globalRarCnts = 0;


  std::filebuf fb;
  if (!fb.open(argv[2], std::ios::in)) {
    std::cerr << "Input file error: " << argv[2] << std::endl;
    exit(1);
  }
  std::istream is(&fb);
  std::string volume;

  while (std::getline(is, volume)) {
    sprintf(filename, "%s/%s.data", argv[1], volume.c_str());

    FILE* fin = fopen(filename, "r");
    if (fin == nullptr) {
      std::cerr << "Analysis of volume " << volume << " missed" << std::endl;
      continue;
    }

    std::cerr << "Processing " << volume << std::endl;

    uint64_t rows;

    // fout1: raw time
    fgets(s, 200, fin);
    sscanf(s, "%lu", &rows);
    takeSecond(fin, fout, fout1_2, volume.c_str(), rows, war2cnt, globalWarCnts);

    // fout3: waw time
    fgets(s, 200, fin);
    sscanf(s, "%lu", &rows);
    takeSecond(fin, fout3, fout3_2, volume.c_str(), rows, rar2cnt, globalRarCnts);

    fclose(fin);
  }
  fclose(fout);
  fclose(fout3);
  fclose(fout1_2);
  fclose(fout3_2);

  FILE* fout1g = fopen("rar_time_global_pct.data", "w");
  FILE* fout3g = fopen("war_time_global_pct.data", "w");
  fprintf(fout1g, "timeInSec pct\n");
  fprintf(fout3g, "timeInSec pct\n");

  std::vector<uint64_t> retVec;
  std::vector<uint64_t> d1, d2;

  {
    for (auto& it : war2cnt) {
      d1.push_back(it.first);
      d2.push_back(it.second);
    }
    retVec = pcts(d1, d2, globalWarCnts);
    int j = 1;
    for (auto& it : retVec) {
      fprintf(fout1g, "%lu %.2lf\n", it, (double)j / 100);
      j++;
    }
  }

  {
    d1.clear(); d2.clear();
    for (auto& it : rar2cnt) {
      d1.push_back(it.first);
      d2.push_back(it.second);
    }
    retVec = pcts(d1, d2, globalRarCnts);
    int j = 1;
    for (auto& it : retVec) {
      fprintf(fout3g, "%lu %.2lf\n", it, (double)j / 100);
      j++;
    }
  }

  fclose(fout1g);
  fclose(fout3g);

  return 0;
}
