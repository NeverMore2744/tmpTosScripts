#include <cstdio>
#include <cstring>
#include <cinttypes>
#include <map>
#include <set>
#include <queue>
#include <utility>

#include "processor.h"

class Processor : Processor_base {
  public:
    void analyze(const char* file_prefix, const char* volume_file) {
      filenames_.push_back("request_size.data");
      filenames_.push_back("overall_request_size.data");
      FILE* fout = fopen(filenames_[0].c_str(), "w");
      FILE* fout2 = fopen(filenames_[1].c_str(), "w");

      std::map<uint64_t, std::pair<uint64_t, uint64_t>> rs2nums;

      fprintf(fout, "log size read_num write_num\n"); 
      fprintf(fout2, "size read_num write_num\n");

      openVolumeFile(volume_file);

      while (std::getline(*is_, volumeId_)) {
        sprintf(filename, "%s/%s.data", file_prefix, volumeId_.c_str());
        fin = fopen(filename, "r");

        if (fin == nullptr) {
          std::cerr << "Analysis of volume " << volumeId_ << " missed" << std::endl;
          continue;
        }

        uint64_t sz, read_num, write_num;
        int ret;
        char* retChar;

        retChar = fgets(s, 200, fin);
        if (retChar == NULL) break;

        while (1) {
          retChar = fgets(s, 200, fin);
          if (retChar == NULL) break;
          ret = sscanf(s, "%lld%lld%lld", &sz, &read_num, &write_num);
          if (ret <= 2) break;

          if (rs2nums.count(sz)) {
            rs2nums[sz].first += read_num;
            rs2nums[sz].second += write_num;
          } else {
            rs2nums[sz] = {read_num, write_num};
          }
          fprintf(fout, "%s %lld %lld %lld\n", logname, sz, read_num, write_num);
        }
        fclose(fin);
      }

      for (auto& it : rs2nums) {
        fprintf(fout2, "%lld %lld %lld\n", it.first, it.second.first, it.second.second);
      }
      fclose(fout);
      fclose(fout2);

      outputFileNames();
    }
};

int main(int argc, char** argv) {
  setbuf(stderr, NULL);
  setbuf(stdout, NULL);

  if (argc < 3) {
    std::cerr << "Error - params not enough. " << argv[0] << " <file_prefix> <volume_file>\n";
    exit(1);
  }

  Processor processor;
  processor.analyze(argv[1], argv[2]);

  return 0;
}
