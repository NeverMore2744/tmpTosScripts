#include <cstdio>
#include <cstring>
#include <iostream>
#include <cassert>
#include <cinttypes>
#include <map>
#include <set>
#include <unordered_set>
#include <queue>
#include "processor.h"

class Processor : Processor_base {
  uint64_t points;
  std::map<uint64_t, uint64_t> freq2cnt;
  std::unordered_set<uint64_t> lbas;
  std::map<uint64_t, std::set<uint64_t>> freq2lbas;

  public:
  void analyze(const char* file_prefix, const char* volume_file) {
    filenames_.push_back("attr.data");
    filenames_.push_back("readFreq.data");
    filenames_.push_back("writeFreq.data");
    filenames_.push_back("rw_only.data");
    FILE* fout = fopen(filenames_[0].c_str(), "w");
    FILE* fout2 = fopen(filenames_[1].c_str(), "w");
    FILE* fout3 = fopen(filenames_[2].c_str(), "w");
    FILE* fout4 = fopen(filenames_[3].c_str(), "w");
    
    std::map<uint64_t, int> lba2readFreq;

    int d[10] = {1, 5, 50, 500, 5000, 50000, 100000, 1000000};
    int cp[10] = {20, 100, 1000, 10000, 100000, 1000000, 10000000, 999999999};

    fprintf(fout, "log wss maxLBA numRReq trb numWReq twb tub urb uwb uub\n");
    fprintf(fout2, "log freq cnt\n");
    fprintf(fout3, "log freq cnt\n");
    fprintf(fout4, "log read_only write_only rw_interleaved read_on_read_only write_on_write_only read_on_others write_on_others\n"); // Each line is an LBA

    openVolumeFile(volume_file);

    while (std::getline(*is_, volumeId_)) {
      sprintf(filename, "%s/%s.data", file_prefix, volumeId_.c_str());
      fin = fopen(filename, "r");

      if (fin == nullptr) {
        std::cerr << "Analysis of volume " << volumeId_ << " missed" << std::endl;
        continue;
      }
      retChar = fgets(s, 200, fin); // First line
      if (retChar == nullptr) {
        std::cerr << "Analysis of volume " << volumeId_ << " missed" << std::endl;
        continue;
      }

      std::cerr << "Processing " << volumeId_ << std::endl;

      uint64_t numRReq, numWReq;

      uint64_t WSS, readWSS, writeWSS, updateWSS = 0; // in blocks
      uint64_t maxLBA; // in blocks
      uint64_t totalReadInBlocks, totalWriteInBlocks, totalUpdateInBlocks;

      uint64_t type = 1;
      uint64_t min, value;
      int cnt = 0;
      int k;

      // Part 1: Basic stats
      fscanf(fin, "%lld%lld", &numRReq, &totalReadInBlocks);
      fscanf(fin, "%lld%lld%lld%lld", &numWReq, &totalWriteInBlocks,
        &totalUpdateInBlocks, &readWSS);

      fgets(s, 200, fin);
      uint64_t addr, times;
      int ret;
      char* retChar;
      maxLBA = 0;

      // Prepare: rw_only
      uint64_t read_only = 0, write_only = 0, rw_interleaved = 0;
      uint64_t read_on_read_only = 0, write_on_write_only = 0, read_on_others = 0, write_on_others = 0;

      // Part 2: read frequency
      lba2readFreq.clear();
      freq2cnt.clear();
      lbas.clear(); 

      while (retChar) {
        ret = sscanf(s, "%lld%lld", &addr, &times);
        if (ret == 1) {
          break; // Next part, writeWSS
        } else {
          assert(ret > 0 && "Write WSS missed. Incorrect input format");
        }

        if (addr > maxLBA) maxLBA = addr;
        lbas.insert(addr);

        lba2readFreq[addr] = times;
        
        // Get freq2cnt
        freq2cnt[times]++;

        retChar = fgets(s, 200, fin);
      }

      writeWSS = addr; 
      for (auto& it : freq2cnt) {
        fprintf(fout2, "%s %lld %lld\n", logname, it.first, it.second);
      }

      // Part 3: fout3: write frequency
      int testNum = 0;
      uint64_t inserted = 0;
      uint64_t threshold = 0;

      freq2lbas.clear();
      freq2cnt.clear();

      while (retChar) {
        retChar = fgets(s, 200, fin);
        if (retChar == NULL) break;
        ret = sscanf(s, "%lld%lld", &addr, &times);
        assert(ret == 2 && "Incorrect input format in write freq");

        if (times > 1) updateWSS++; 
        testNum++;
        if (addr > maxLBA) maxLBA = addr;
        lbas.insert(addr);

        // Part 4. consider the read/write only blocks
        int rt = (lba2readFreq.count(addr)) ? lba2readFreq[addr] : 0;
        int wt = times;
        uint32_t sum_num = rt + wt;
        if ((double)rt / sum_num <= 0.05) {  // Write-most
          write_only++, write_on_write_only += wt, read_on_others += rt;
        } else if ((double)wt / sum_num <= 0.05) {  // Read-most
          read_only++, read_on_read_only += rt, write_on_others += wt;
        } else { // Interleaved
          rw_interleaved++, read_on_others += rt, write_on_others += wt;
        }

        if (lba2readFreq.count(addr)) lba2readFreq.erase(addr);

        // 3.2 Get freq2cnt
        freq2cnt[times]++;
      }
      for (auto& it : freq2cnt) {
        fprintf(fout3, "%s %lld %lld\n", logname, it.first, it.second);
      }

      for (auto& it : lba2readFreq) {
        read_only++, read_on_read_only += it.second;
      }

      fprintf(fout4, "%s %llu %llu %llu %llu %llu %llu %llu\n", logname,
          read_only, write_only, rw_interleaved,
          read_on_read_only, write_on_write_only, read_on_others, write_on_others);

      WSS = lbas.size();
      assert(testNum == writeWSS);
      assert(WSS <= readWSS + writeWSS);
      assert(writeWSS >= updateWSS);

      fprintf(fout, "%s %lld %lld %lld %lld %lld %lld %lld %lld %lld %lld\n", 
          logname, WSS, maxLBA, numRReq, totalReadInBlocks, numWReq, totalWriteInBlocks, totalUpdateInBlocks, 
          readWSS, writeWSS, updateWSS); 

      fclose(fin);
    }

    fclose(fout);
    fclose(fout2);
    fclose(fout3);
    fclose(fout4);

    outputFileNames();
  }
};

int main(int argc, char** argv) {
  setbuf(stdout, NULL);
  setbuf(stderr, NULL);

  if (argc < 3) {
    std::cerr << "Error - params not enough. " << argv[0] << " <file_prefix> <volume_file>\n";
    exit(1);
  }

  Processor processor;
  processor.analyze(argv[1], argv[2]);
}
