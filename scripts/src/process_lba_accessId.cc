#include <stdio.h>
#include <string.h>
#include <cassert>
#include <inttypes.h>
#include <assert.h>
#include <vector>
#include <map>
#include <set>
#include <algorithm>
#include <inttypes.h>

uint64_t gcd(uint64_t a, uint64_t b) {
  if (b == 0) return a;
  if (a % b == 0) return b;
  return gcd(b, a % b);
}

bool isDigitString(std::string str) {
  return std::all_of(str.begin(), str.end(), ::isdigit);
}

int main(int argc, char** argv) {
  int num_logs = 0;
  uint64_t lbaStart = 0, lbaEnd = 0, lbaInterval = 0;
  uint64_t timeInMinStart = 0, timeInMinEnd = 0, timeInMinInterval = 0;
  bool freeMode = false;
  int startArgc = 1;

  if (isDigitString(std::string(argv[1]))) {
    sscanf(argv[1], "%I64u", &timeInMinStart);
    sscanf(argv[2], "%I64u", &timeInMinEnd); // Zero: Use all times
    sscanf(argv[3], "%I64u", &timeInMinInterval);  // Zero: Use 10 as the interval
    sscanf(argv[4], "%I64u", &lbaStart);
    sscanf(argv[5], "%I64u", &lbaEnd); // Zero: Use all LBAs
    sscanf(argv[6], "%I64u", &lbaInterval);  // Zero: Use 1024 intervals
    startArgc = 7;
  } else {
    freeMode = true;
  }

  if (lbaEnd > lbaStart && lbaInterval == 0) {
    lbaInterval = (lbaEnd - lbaStart >= 1024) ? (lbaEnd - lbaStart) / 1024 : 1;
  }
  if (timeInMinEnd > timeInMinStart && timeInMinInterval == 0) {
    timeInMinInterval = (timeInMinEnd - timeInMinStart >= 10) ? 10 : 1;
  }

  if (!freeMode) {
    printf("%I64u %I64u %I64u (lba)\n", lbaStart, lbaEnd, lbaInterval);
    printf("%I64u %I64u %I64u (time)\n", timeInMinStart, timeInMinEnd, timeInMinInterval);
  } else {
    printf("freeMode\n");
  }

  for (int i = startArgc; i < argc; i++) {
    FILE* fin = fopen(argv[i], "r");
    char s[50], logname[50], filename[50];
    strcpy(s, argv[i]);
    for (int j = 0; j < (int)strlen(s); j++) { 
      if (s[j] == '/' || s[j] == '.') {
        s[j] = '\0';
        break;
      }
    }
    strcpy(logname, s);
    strcpy(filename, logname);
    strcat(filename, "_matrix.mtx");

    if (fin == NULL) {
      printf("File not exist: %s\n", argv[i]);
      continue;
    }

    printf("%d: %s\n", ++num_logs, logname);

    uint64_t lba1 = lbaStart, lba2 = lbaEnd, lbaInt = lbaInterval;
    uint64_t time1 = timeInMinStart, time2 = timeInMinEnd, timeInt = timeInMinInterval;
    uint64_t row, col;

    std::map<uint64_t, std::map<uint64_t, uint64_t>> time2lbaCnt;

    uint64_t time, lba, cnt, sizes;  // "time" and "lba" are aligned using multiple minutes or multiple blocks
    bool first = true, fileCorrupted = false;

    // freeMode == false: Intervals known, Save part of the mappings
    // freeMode == true: Save all of the mappings, print directly
    while (fscanf(fin, "%I64u%I64u", &time, &sizes) == 2 && !fileCorrupted) {
      if (!freeMode) {
        if (time > time2) break;
        time = (time - time1) / timeInt * timeInt;
        for (int i = 0; i < (int)sizes; i++) {
          int ret = fscanf(fin, "%I64u%I64u", &lba, &cnt);
          if (ret != 2) {
            printf("Error in file %s ... exiting\n", logname);
            fileCorrupted = true;
            break;
          }
          lba = (lba - time1) / lbaInt * lbaInt;
          time2lbaCnt[time][lba] += cnt;
        }

        first = false;

      } else { // Need to print every thing
        if (first) time1 = time2 = time;
        else {
          time1 = std::min(time1, time);
          time2 = std::max(time2, time);
          if (timeInt == 0 && time2 > time1) timeInt = time2 - time1;
          else timeInt = gcd(timeInt, gcd(time2 - time1, time - time1));
        }
        for (int i = 0; i < (int)sizes; i++) {
          int ret = fscanf(fin, "%I64u%I64u", &lba, &cnt);

          if (ret != 2) {
            printf("Error in file %s ... exiting\n", logname);
            fileCorrupted = true;
            break;
          }

          if (first) lba1 = lba2 = lba, first = false;
          else {
            lba1 = std::min(lba1, lba);
            lba2 = std::max(lba2, lba);
            if (lbaInt == 0 && lba2 > lba1) lbaInt = lba2 - lba1;
            else lbaInt = gcd(lbaInt, gcd(lba2 - lba1, lba - lba1));
          }
          time2lbaCnt[time][lba] += cnt;
        }

      }
    }

    if (first == true) {
      printf("Error: data file empty\n");
      continue;
    }

    // Start to print
    FILE* fout = fopen(filename, "w"); // Sparse matrix
    fprintf(fout, "%%%%MatrixMarket matrix coordinate real general\n");
    fprintf(fout, "%% %I64u %I64u %I64u\n", time1, time2, timeInt);
    fprintf(fout, "%% %I64u %I64u %I64u\n", lba1, lba2, lbaInt);

    row = time2 / timeInt + 1;
    col = lba2 / lbaInt + 1;

    cnt = 0;
    for (auto& it : time2lbaCnt) {
      cnt += it.second.size();
    }
    fprintf(fout, "%I64u %I64u %I64u\n", row, col, cnt);

    printf("%% %I64u %I64u %I64u ; ", time1, time2, timeInt);
    printf("%I64u %I64u %I64u ; %I64u\n", lba1, lba2, lbaInt, cnt);

    for (auto& it : time2lbaCnt) {
      uint64_t timeAligned = (it.first - time1) / timeInt + 1; 
      for (auto& it0 : it.second) {
        assert(it.first + 1 <= row);
        assert(it0.first + 1 <= col);
        fprintf(fout, "%I64u %I64u %I64u\n", it.first + 1, it0.first + 1, it0.second); // , (it0.first - lba1) / lbaInt + 1, it0.second);
      }
    }
    printf("--------------------\n");

    fclose(fin);
    fclose(fout);
  }

  return 0;
}
