#include <cstdio>
#include <cstring>
#include <iostream>
#include <cassert>
#include <cinttypes>
#include <map>
#include <set>
#include <unordered_set>
#include <queue>

int main(int argc, char** argv) {
  setbuf(stdout, NULL);

  FILE* fout = fopen("attr.data", "w");
  FILE* fout2 = fopen("readFreq.data", "w");
  FILE* fout3 = fopen("writeFreq.data", "w");
  FILE* fout4 = fopen("rw_only.data", "w");
  int num_logs = 0;

  uint64_t points;
  std::map<uint64_t, uint64_t> freq2cnt;
  std::unordered_set<uint64_t> lbas;

  std::map<uint64_t, std::set<uint64_t>> freq2lbas;
  std::map<uint64_t, int> lba2read;

  int d[10] = {1, 5, 50, 500, 5000, 50000, 100000, 1000000};
  int cp[10] = {20, 100, 1000, 10000, 100000, 1000000, 10000000, 999999999};

  fprintf(fout, "log wss maxLBA numRReq trb numWReq twb tub urb uwb uub\n");
  fprintf(fout2, "log freq cnt\n");
  fprintf(fout3, "log freq cnt\n");
  fprintf(fout4, "log read_only write_only rw_interleaved read_on_read_only write_on_write_only read_on_others write_on_others\n"); // Each line is an LBA

  for (int i = 1; i < argc; i++) 
//  for (int fn = 1058, i = 0; fn <= 26075; fn++)
  {
    char filename[200];
//    sprintf(filename, "%d.data", fn);
    sprintf(filename, "%s", argv[i]);

    FILE* fin = fopen(filename, "r");
    if (fin == nullptr) {
      continue;
    }
//    i++;

    char s[200], logname[200];
    strcpy(s, filename);

    for (int j = 0; j < strlen(s); j++) {
      if (s[j] == '.' || s[j] == '/') {
        s[j] = '\0';
        break;
      }
    }
    strcpy(logname, s);

    printf("%d: %s\n", i, logname);

    uint64_t numRReq, numWReq;

    uint64_t WSS, readWSS, writeWSS, updateWSS = 0; // in blocks
    uint64_t maxLBA; // in blocks
    uint64_t totalReadInBlocks, totalWriteInBlocks, totalUpdateInBlocks;

    uint64_t type = 1;
    uint64_t min, value;
    int cnt = 0;
    int k;

    fscanf(fin, "%lld%lld", &WSS, &maxLBA);
    fscanf(fin, "%lld%lld", &numRReq, &totalReadInBlocks);
    fscanf(fin, "%lld%lld%lld%lld", &numWReq, &totalWriteInBlocks,
      &totalUpdateInBlocks, &readWSS);

//    printf("   %lld %lld | %lld %lld | %lld %lld \n", 
//        numRReq, totalReadInBlocks,
//        numWReq, totalWriteInBlocks, 
//        totalUpdateInBlocks, readWSS);

    fgets(s, 200, fin);
    uint64_t addr, times;
    int ret;
    char* retChar;
    maxLBA = 0;

    // Prepare: rw_only
    uint64_t read_only = 0, write_only = 0, rw_interleaved = 0;
    uint64_t read_on_read_only = 0, write_on_write_only = 0, read_on_others = 0, write_on_others = 0;

    // Part 2: fout2: read frequency
    lba2read.clear();
    freq2cnt.clear();
    lbas.clear(); 

    while (1) {
      retChar = fgets(s, 200, fin);
      if (retChar == NULL) break;
      ret = sscanf(s, "%lld%lld", &addr, &times);
      if (ret <= 1) break;
      if (addr > maxLBA) maxLBA = addr;
      lbas.insert(addr);

      lba2read[addr] = times;
      
      // Get freq2cnt
      freq2cnt[times]++;
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
    while (1) {
      retChar = fgets(s, 200, fin);
      if (retChar == NULL) break;
      ret = sscanf(s, "%lld%lld", &addr, &times);
      if (ret <= 1) break;

      if (times > 1) updateWSS++; 
      testNum++;
      if (addr > maxLBA) maxLBA = addr;
      lbas.insert(addr);

      // Part 4. consider the read/write only blocks
      int rt = (lba2read.count(addr)) ? lba2read[addr] : 0;
      int wt = times;
      uint32_t sum_num = rt + wt;
      if ((double)rt / sum_num <= 0.05) write_only++, write_on_write_only += wt, read_on_others += rt;
      else if ((double)wt / sum_num <= 0.05) read_only++, read_on_read_only += rt, write_on_others += wt;
      else rw_interleaved++, read_on_others += rt, write_on_others += wt;
      if (lba2read.count(addr)) lba2read.erase(addr);

      // 3.2 Get freq2cnt
      freq2cnt[times]++;
    }
    for (auto& it : freq2cnt) {
      fprintf(fout3, "%s %lld %lld\n", logname, it.first, it.second);
    }

    for (auto& it : lba2read) {
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
  return 0;
}
