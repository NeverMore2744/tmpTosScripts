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

  void analyze(char *inputTrace, char* outputPrefix) {
      uint64_t offset, length, timestamp;
      char type;
      char filename[300];
      uint64_t timeCalculated;

      std::map<int, std::vector<std::string>> leftStrings;
      int res;  // volume Id

      uint64_t cnt = 0, saved = 0;
      timeval tv1, tv2;
      gettimeofday(&tv1, NULL);

      std::vector<std::string> strs; 

      std::string line;
      std::filebuf fb;
      if (!fb.open(inputTrace,std::ios::in)) {
        std::cout << "Input file error" << std::endl;
        return;
      }
      std::istream is(&fb);
      char line2[200];
      
      while (std::getline(is, line)) {
        strcpy(line2, line.c_str());
        int pos = strlen(line2) - 1;
        for ( ; pos >= 0 && line2[pos] != ','; pos--); 
        std::string s = std::string(line2, pos);
        strs.push_back(s);

        cnt++;
        if (cnt % 100000 == 0) printf("%d\n", cnt);
        if (cnt >= 20000000) break;
      }
      fb.close();
      gettimeofday(&tv2, NULL);
      printf("%d seconds\n", tv2.tv_sec - tv1.tv_sec);

      std::ofstream myfile;
      myfile.open("example.txt", std::ofstream::app | std::ofstream::out);
      for(auto& it : strs) {
        myfile << it << "\n";
      }
      myfile.close();
      gettimeofday(&tv2, NULL);
      printf("%d seconds\n", tv2.tv_sec - tv1.tv_sec);
  }
};

int main(int argc, char *argv[]) {
  Split split;
  split.analyze(argv[1], argv[2]);
}
