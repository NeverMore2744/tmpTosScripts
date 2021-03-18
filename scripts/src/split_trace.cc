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

class Split {
  Trace trace;

  uint64_t startTimestamp_ = 0;
  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;

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

        int res = 0;
        for (int i = pos+1; line2[i] != '\0'; i++) if (isdigit(line2[i])) res = res*10 + (line2[i] - '0');

        leftStrings[res].push_back(s);
        saved++;
        cnt++;
        if (cnt % 100000 == 0) {
          std::cout << cnt << " " << saved << std::endl;
          gettimeofday(&tv2, NULL);
          std::cout << tv2.tv_sec - tv1.tv_sec << " seconds " << std::endl; 
        }

        if (cnt % 80000000 == 0) {
          int ct = 0;
          for(auto& it : leftStrings) {
            if (ct % 100 == 0) {
              std::cout << "writing file " << it.first << ".csv" << std::endl;
            }

            sprintf(filename, "%s/%d.csv", outputPrefix, it.first);
            std::ofstream fs;
            fs.open(filename, std::ofstream::out | std::ofstream::app);
            for(auto& it0 : it.second) {
              fs << it0 << "\n";
            }
            fs.close();

            ct++;
          }
          leftStrings.clear();
          saved = 0;
          gettimeofday(&tv2, NULL);
          std::cout << tv2.tv_sec - tv1.tv_sec << " seconds " << std::endl; 
        }
      }

      for(auto& it : leftStrings) {
        sprintf(filename, "%s/%d.csv", outputPrefix, it.first);
        std::ofstream fs;
        fs.open(filename, std::ofstream::out | std::ofstream::app);
        for(auto& it0 : it.second) {
          fs << it0 << "\n";
        }
        fs.close();
      }

      fb.close();
  }
};

int main(int argc, char *argv[]) {
  Split split;
  split.analyze(argv[1], argv[2]);
}
