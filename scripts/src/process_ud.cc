#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <vector>

// MSR: use minute
// Ali: use second

int main(int argc, char** argv) {
  setbuf(stdout, NULL);

  FILE* fout = fopen("ud_time.data", "w");
  FILE* fout2 = fopen("ud_amount.data", "w");
  int num_logs = 0;

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

//  for (int i = 1; i < argc; i++) 
  for (int i = 1000; i <= 26075; i++)
  {
    char filename[200], tmps[200];
    sprintf(filename, "%d.data", i);
//    sprintf(filename, "%s", argv[i]);

    FILE* fin = fopen(filename, "r");
    if (fin == nullptr) continue;

    char s[200], logname[200];
    strcpy(s, filename);
    for (int j = 0; j < strlen(s); j++) {
      if (s[j] == '/' || s[j] == '.') {
        s[j] = '\0';
        break;
      }
    }
    strcpy(logname, s);

    printf("%d: %s, ", ++num_logs, logname);

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
  return 0;
}
