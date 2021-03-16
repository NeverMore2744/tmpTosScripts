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

class Analyzer {
  Trace trace;

  uint64_t nBlocks_ = -1ull;
  uint64_t currentId_ = 0;

  uint64_t get_wss(uint64_t* arr, uint64_t arr_size) {
    uint64_t sum = 0, t;
    for (int i = 0; i < arr_size; i++) {
      t = arr[i];
      while (t > 0) {
        sum += t % 2;
        t >>= 1;
      }
    }
    return sum;
  }

public:

  void analyze(char *inputTrace, uint64_t inputMaxLba) {
      uint64_t offset, length, timestamp;
      char type;
      char filename[300];
      uint64_t timeCalculated;

      int res;  // volume Id

      uint64_t cnt = 0, saved = 0;
      timeval tv1, tv2;
      gettimeofday(&tv1, NULL);

      int size_array = (inputMaxLba + 512) / 512; //1024 * 1024 * 4;
      uint64_t* wss = new uint64_t[size_array];
      uint64_t* rwss = new uint64_t[size_array];
      uint64_t* wwss = new uint64_t[size_array];
      uint64_t* uwss = new uint64_t[size_array];
      memset(wss, 0, sizeof(uint64_t) * size_array);
      memset(rwss, 0, sizeof(uint64_t) * size_array);
      memset(wwss, 0, sizeof(uint64_t) * size_array);
      memset(uwss, 0, sizeof(uint64_t) * size_array);

      std::string line;
      std::filebuf fb;
      if (!fb.open(inputTrace, std::ios::in)) {
        std::cout << "Input file error: " << inputTrace << std::endl;
        return;
      }
      std::istream is(&fb);

      char line2[200];
      uint64_t maxLba = 0, minLba = 0;
      bool first = true;

      while (trace.readNextRequestFstream(is, timestamp, type, offset, length, line2)) {
        for (uint64_t i = offset; i < offset + length; i++) {
          wss[i / 64] |= ((uint64_t)1) << (i % 64); 
          if (type == 'R') {
            rwss[i / 64] |= ((uint64_t)1) << (i % 64); 
          } else {
            if (wwss[i / 64] & (((uint64_t)1) << (i % 64))) {
              uwss[i / 64] |= ((uint64_t)1) << (i % 64); 
            }
            wwss[i / 64] |= ((uint64_t)1) << (i % 64); 
          }
        }

        if (first) {
          maxLba = offset + length - 1;
          minLba = offset;
          first = false;
        } else {
          if (offset + length - 1 > maxLba) maxLba = offset + length - 1;
          if (offset < minLba) minLba = offset;
        }

        cnt++;
        if (cnt % 1000000 == 0) {
          std::cerr << cnt << " ";
          gettimeofday(&tv2, NULL);
          std::cerr << tv2.tv_sec - tv1.tv_sec << " seconds " << std::endl; 
        }
      }

      std::cout << inputTrace << " " << get_wss(wss, size_array) 
        << " " << get_wss(rwss, size_array) 
        << " " << get_wss(wwss, size_array) 
        << " " << get_wss(uwss, size_array) 
        << " " << minLba << " " << maxLba << std::endl;

      delete wss;
      delete rwss;
      delete wwss;
      delete uwss;

      fb.close();
  }
};

int main(int argc, char *argv[]) {
  Analyzer analyzer;
  uint64_t maxLba;
  if (argc < 3) {
    std::cerr << "Input error" << std::endl;
    return 1;
  }
  sscanf(argv[2], "%llu", &maxLba);
  std::cerr << maxLba << std::endl;

  analyzer.analyze(argv[1], maxLba);
  return 0;
}
