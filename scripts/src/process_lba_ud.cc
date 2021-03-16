#include <stdio.h>
#include <string.h>
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

int getBits(uint64_t value) {
  int bits = 0;
  while (value > 1) {
    bits++;
    value >>= 1;
  }
  return bits;
}

int main(int argc, char** argv) {
  int num_logs = 0;
  uint64_t lbaStart = 0, lbaEnd = 0, lbaInterval = 0;
  bool freeMode = false;
  int startArgc = 1;

  if (isDigitString(std::string(argv[1]))) {
    sscanf(argv[1], "%I64u", &lbaStart);
    sscanf(argv[2], "%I64u", &lbaEnd); // Zero: Use all LBAs
    sscanf(argv[3], "%I64u", &lbaInterval);  // Zero: Use 1024 intervals
    startArgc = 4;
  } else {
    freeMode = true;
  }

  if (lbaEnd > lbaStart && lbaInterval == 0) {
    lbaInterval = (lbaEnd - lbaStart >= 1024) ? (lbaEnd - lbaStart) / 1024 : 1;
  }

  if (!freeMode) {
    printf("%I64u %I64u %I64u (lba)\n", lbaStart, lbaEnd, lbaInterval);
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
    uint64_t row, col;

    std::map<uint64_t, std::map<uint64_t, uint64_t>> lba2udCnt;

    uint64_t ud, lba, cnt, sizes, maxBits = 0; 
    bool first = true, fileCorrupted = false;

    // freeMode == false: Intervals known, Save part of the mappings
    // freeMode == true: Save all of the mappings, print directly
    while (fscanf(fin, "%I64u%I64u", &lba, &sizes) == 2 && !fileCorrupted) {
      if (!freeMode) {
        for (int i = 0; i < (int)sizes; i++) {
          int ret = fscanf(fin, "%I64u%I64u", &ud, &cnt);
          if (ret != 2) {
            printf("Error in file %s ... exiting\n", logname);
            fileCorrupted = true;
            break;
          }
          ud = getBits(ud);
          lba2udCnt[lba][ud] += cnt;
        }

        first = false;

      } else { // Need to print every thing
        for (int i = 0; i < (int)sizes; i++) {
          int ret = fscanf(fin, "%I64u%I64u", &ud, &cnt);
          if (ret != 2) {
            printf("Error in file %s ... exiting\n", logname);
            fileCorrupted = true;
            break;
          }

          ud = getBits(ud); 
          if (ud > maxBits) maxBits = ud;
          if (first) lba1 = lba2 = lba, first = false;
          else {
            lba1 = std::min(lba1, lba);
            lba2 = std::max(lba2, lba);
            if (lbaInt == 0 && lba2 > lba1) lbaInt = lba2 - lba1;
            else lbaInt = gcd(lbaInt, gcd(lba2 - lba1, lba - lba1));
          }
          lba2udCnt[lba][ud] = cnt;
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
    fprintf(fout, "%% %I64u %I64u %I64u\n", lba1, lba2, lbaInt);

    row = maxBits + 1; 
    col = (lba2 - lba1) / lbaInt + 1;
    cnt = 0;
    for (auto& it : lba2udCnt) {
      cnt += it.second.size();
    }
    fprintf(fout, "%I64u %I64u %I64u\n", row, col, cnt);

    printf("%I64u %I64u %I64u ; %I64u\n", lba1, lba2, lbaInt, cnt);

    for (auto& it : lba2udCnt) {
      for (auto& it0 : it.second) {
        fprintf(fout, "%I64u %I64u %I64u\n", it0.first + 1, (it.first - lba1) / lbaInt + 1, it0.second);
      }
    }
    printf("--------------------\n");

    fclose(fin);
    fclose(fout);
  }

  return 0;
}
