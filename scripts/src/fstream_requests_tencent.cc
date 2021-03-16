#include <iostream>
#include <fstream>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <map>
#include <cstdio>
#include <vector>
#include <set>
#include <algorithm>
#include "large_array.h"
#include "trace.h"
#include <string>
#include <cstdlib>
#include <sys/time.h>
#include <unistd.h>

#define INTV_TO_MINUTE 600000000
#define MAX_MINUTE (7 * 24 * 60)

class Split {
  Trace trace;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;
  uint64_t beginTimestampInMin_ = 12816637200llu / 60; // Minimum timestamp in MSR 

public:

  void analyze(char *inputTrace) {
      uint64_t offset, length, timestamp;
      char type;
      char filename[300];
      uint64_t timeCalculated;

      uint64_t rsecs = 0, wsecs = 0, rreqs = 0, wreqs = 0;
      int res;  // volume Id

      uint64_t cnt = 0, saved = 0;
      timeval tv1, tv2;
      gettimeofday(&tv1, NULL);

      std::string line;
      std::filebuf fb;
      if (!fb.open(inputTrace,std::ios::in)) {
        std::cout << "Input file error: " << inputTrace << std::endl;
        return;
      }
      std::istream is(&fb);
      char line2[200];

      while (std::getline(is, line)) {
        strcpy(line2, line.c_str());
        int pos = strlen(line2) - 1;
        for ( ; pos >= 0 && line2[pos] != ','; pos--); pos++; 
        int pos2 = pos-2;
        for ( ; pos2 >= 0 && line2[pos2] != ','; pos2--); pos2++;

        int para3 = 0, para4 = line2[pos] - '0';
        for (int i = pos2; isdigit(line2[i]); i++) para3 = para3*10 + (line2[i] - '0');

        if (para4 == 0) {
          rsecs += para3;
          rreqs++;
        } else {
          wsecs += para3;
          wreqs++;
        }

        cnt++;
        if (cnt % 1000000 == 0) {
          std::cerr << cnt << std::endl;
          gettimeofday(&tv2, NULL);
          std::cerr << tv2.tv_sec - tv1.tv_sec << " seconds " << std::endl; 
        }
      }

      std::cout << inputTrace << " " << rsecs << " " << wsecs << " " << rreqs << " " << wreqs << std::endl;

      fb.close();
  }
};

int main(int argc, char *argv[]) {
  Split split;
  split.analyze(argv[1]);
}
