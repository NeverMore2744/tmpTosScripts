#include <stdio.h>
#include <fstream>
#include <cmath>
#include <iostream>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <vector>
#include <map>

#include "processor.h"

class Processor : Processor_base {
  public:

  void process_cv(FILE* fout, uint64_t rows) {
    char s[200];
    uint64_t cv_index2, times;
    for (int i = 0; i < rows; i++) {
      retChar = fgets(s, 200, fin);
      if (retChar == NULL) break;
      ret = sscanf(s, "%lu %lu", &cv_index2, &times);
      assert(ret == 2);

      fprintf(fout, "%s %.6lf %lu\n", volumeId_.c_str(), power(2, ((double)cv_index2 - 50.0) / 10), times);
    }
  }

  void process_udInGb(FILE* fout, uint64_t rows) {
    char s[200];
    uint64_t udInGb, times;

    for (int i = 0; i < rows; i++) {
      retChar = fgets(s, 200, fin);
      if (retChar == NULL) break;
      ret = sscanf(s, "%lu %lu", &udInGb, &times);
      assert(ret == 2);

      fprintf(fout, "%s %.6lf %lu\n", volumeId_.c_str(), udInGb, times);
    }
  }

  void analyze(const char* file_prefix, const char* volume_file) {
    FILE* fout = fopen("cv.data", "w");
    FILE* fout2 = fopen("hot_lifespan.data", "w");
    FILE* fout3 = fopen("hot_cnt.data", "w");
    fprintf(fout, "log cv cnts\n");
    fprintf(fout2, "log udInGb cnts\n");
    fprintf(fout3, "log hot cold cnts\n");

    openVolumeFile(volume_file);
    while (std::getline(*is_, volumeId_)) {
      sprintf(filename, "%s/%s.data", file_prefix, volumeId_.c_str());
      FILE* fin = fopen(filename, "r");
      char* retChar;

      if (fin == nullptr) {
        std::cerr << "Analysis of volume " << volume << " missed" << std::endl;
        continue;
      }
      std::cerr << "Processing " << volume << std::endl;

      // fout: cv values 
      retChar = fgets(s, 200, fin);
      if (retChar == NULL) break;
      sscanf(s, "%lu", &rows);
      process_cv(fout, rows);

      // fout2: udInGb values 
      retChar = fgets(s, 200, fin);
      if (retChar == NULL) break;
      sscanf(s, "%lu", &rows);
      process_cv(fout2, rows);

      fclose(fin);
    }

    fclose(fout);
    fclose(fout2);

    std::cout << "cv.data\nhot_lifespan.data\n";
  }
};

int main(int argc, char** argv) {
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  Processor processor;

  if (argc < 3) {
    std::cerr << "Error - params not enough. " << argv[0] << " <file_prefix> <volume_file>\n";
    exit(1);
  }

  processor.analyze(argv[1], argv[2]);
}
