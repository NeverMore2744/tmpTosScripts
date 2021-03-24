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

  void process_cv(FILE* fin, FILE* fout, int pct, uint64_t lbaCnts, uint64_t rows) {
    char s[200], *retChar;
    uint64_t cv_index2, times;
    double *cv_values = new double[rows];
    uint64_t *times_values = new uint64_t[rows];

    uint64_t tmpLbaCnt = 0;
    int ret;

    for (int i = 0; i < rows; i++) {
      retChar = fgets(s, 200, fin);
      assert(retChar != NULL && "file not complete: stop at process_cv");
      ret = sscanf(s, "%lu %lu", &cv_index2, &times);
      assert(ret == 2 && "not enough inputs: stop at process_cv");

      cv_values[i] = pow(2, ((double)cv_index2 - 50.0) / 10);
      times_values[i] = times;
      tmpLbaCnt += times;
    }

    for (int i = 0; i < rows; i++) {
      fprintf(fout, "%s %d %lu %lu %.6lf %lu\n", volumeId_.c_str(), pct, lbaCnts, tmpLbaCnt,
          cv_values[i], times_values[i]);
    }

    delete cv_values;
    delete times_values;
  }

  void process_udInGb(FILE* fin, FILE* fout, int pct, uint64_t lbaCnts, uint64_t rows) {
    char s[200], *retChar;
    uint64_t udInGb, times;

    uint64_t *udInGb_values = new uint64_t[rows];
    uint64_t *times_values = new uint64_t[rows];
    int ret;

    for (int i = 0; i < rows; i++) {
      retChar = fgets(s, 200, fin);
      assert(retChar != NULL && "file not complete: stop at process_udInGb");
      ret = sscanf(s, "%lu %lu", &udInGb, &times);
      assert(ret == 2 && "not enough inputs: stop at process_udInGb");

      udInGb_values[i] = udInGb;
      times_values[i] = times;
    }

    for (int i = 0; i < rows; i++) {
      fprintf(fout, "%s %d %lu %lu %lu\n", volumeId_.c_str(), pct, lbaCnts, 
          udInGb_values[i], times_values[i]);
    }

    delete udInGb_values;
    delete times_values;
  }

  void analyze(const char* file_prefix, const char* volume_file) {
    FILE* fout = fopen("pct2freq.data", "w");
    FILE* fout2 = fopen("hot_cv.data", "w");
    FILE* fout3 = fopen("hot_lifespan.data", "w");
    fprintf(fout, "log pct freq\n");
    fprintf(fout2, "log pct lbaCnt actLbaCnt cv cnts\n");
    fprintf(fout3, "log pct cnt lifespanInGb cnts\n");

    openVolumeFile(volume_file);

    int pct = 0, ret = 0;
    uint64_t rows, lbaCnts, lbaFreq;
    FILE* fin;
    char* retChar, s[200], filename[200];

    while (std::getline(*is_, volumeId_)) {
      pct = 0;

      sprintf(filename, "%s/%s.data", file_prefix, volumeId_.c_str());
      fin = fopen(filename, "r");

      if (fin == nullptr) {
        std::cerr << "Analysis of volume " << volumeId_ << " missed" << std::endl;
        continue;
      }
      std::cerr << "Processing " << volumeId_ << std::endl;

      retChar = fgets(s, 200, fin);
      while (retChar != NULL) {
        // Part 1: pct2freq.data
        ret = sscanf(s, "%llu %llu", &lbaCnts, &lbaFreq);
        if (ret < 2) { // final
          pct++;
          break;
        }
        fprintf(fout, "%s %d %lu\n", volumeId_.c_str(), pct, lbaFreq);

        // Part 2: hot_cv.data
        retChar = fgets(s, 200, fin);
        assert(retChar != NULL && "file not complete: stop at part 2");
        sscanf(s, "%lu", &rows);
        process_cv(fin, fout2, pct, lbaCnts, rows);

        // Part 3: hot_lifespan.data
        retChar = fgets(s, 200, fin);
        assert(retChar != NULL && "file not complete: stop at part 3");
        sscanf(s, "%lu", &rows);
        process_udInGb(fin, fout3, pct, lbaCnts, rows);

        retChar = fgets(s, 200, fin);
        pct++;
      }

      // Final: cv values 
      sscanf(s, "%lu", &rows);
      process_cv(fin, fout2, pct, 0, rows);

      // fout2: udInGb values 
      retChar = fgets(s, 200, fin);
      assert(retChar != NULL && "file not complete: stop finally");
      sscanf(s, "%lu", &rows);
      process_cv(fin, fout3, pct, 0, rows);

      fclose(fin);
    }

    fclose(fout);
    fclose(fout2);
    fclose(fout3);

    std::cout << "pct2freq.data\nhot_cv.data\nhot_lifespan.data";
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
