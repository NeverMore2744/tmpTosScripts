#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>

int d[10] = {1, 10, 100, 1000, 10000, 100000, 1000000};
int cp[10] = {100, 1000, 10000, 100000, 1000000, 10000000, 999999999};
int dt[10] = {1, 60, 600, 6000, 60000, 600000, 6000000, 60000000};
int cpt[10] = {600, 6000, 60000, 600000, 6000000, 60000000, 600000000, 999999999};

int intv = 1;  // 10 if it is MSR

void takeSecond(FILE* fin, FILE* fout, char* logname, double coefficient) {
  char* retChar;
  char s[200];
  uint64_t cnt;
  int ret, time;
  int lastZeroIndex = -1;
  uint64_t arraySize;

  coefficient = coefficient * intv;

  retChar = fgets(s, 200, fin);
  if (retChar == NULL) {
    printf("Error!\n");
    exit(1);
  }

  uint64_t sum = 0;
  const uint64_t cut = 1;

  sscanf(s, "%lld", &arraySize);
  printf("arraySize = %lld\n", arraySize);
  for (uint64_t i = 0; i < arraySize; i++) {
    retChar = fgets(s, 200, fin);
    if (retChar == NULL) break;
    ret = sscanf(s, "%d %llu", &time, &cnt);
    assert(ret == 2);

//    sum += cnt;
//    if (i % 2 != cut) {
//      continue;
//    }

//    if (i >= cut && i < arraySize - 1 && sum == 0) {
//      lastZeroIndex = i;
//    } else {
//      if (lastZeroIndex >= 0) {
////        fprintf(fout, "%s %.6lf 0\n", logname, (double)lastZeroIndex / coefficient);
//        lastZeroIndex = -1;
//      }
      fprintf(fout, "%s %.7lf %lld\n", logname, (double)time / coefficient, cnt);
//    } 
//    sum = 0;
  }
}

int main(int argc, char** argv) {
  setbuf(stdout, NULL);

  FILE* fout = fopen("iat2.data", "w");
  int num_logs = 0;
  fprintf(fout, "log timeInSec cnts\n"); 

  for (int fn = 1058, i = 0; fn <= 26075; fn++)
  {
    char filename[200];
    sprintf(filename, "%d.data", fn);
//    sprintf(filename, "%s", argv[i]);

    FILE* fin = fopen(filename, "r");
    if (fin == nullptr) continue;
    i++;

    char s[200], logname[200];
    strcpy(s, filename);

    for (int j = 0; j < strlen(s); j++) {
      if (s[j] == '.' || s[j] == '/') {
        s[j] = '\0';
        break;
      }
    }
    strcpy(logname, s);
  
    printf("%d: %s, ", ++num_logs, logname);
    
    takeSecond(fin, fout, logname, 10000000.0);
    takeSecond(fin, fout, logname, 1000000.0);
    takeSecond(fin, fout, logname, 10000.0);
    takeSecond(fin, fout, logname, 100.0);
    takeSecond(fin, fout, logname, 10.0);
    takeSecond(fin, fout, logname, 1);
    fclose(fin);
  }
  fclose(fout);
  return 0;
}
