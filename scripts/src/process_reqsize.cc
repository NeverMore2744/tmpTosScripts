#include <cstdio>
#include <cstring>
#include <cinttypes>
#include <map>
#include <set>
#include <queue>
#include <utility>

int main(int argc, char** argv) {
  setbuf(stdout, NULL);

  FILE* fout = fopen("request_size.data", "w");
  FILE* fout2 = fopen("overall_request_size.data", "w");
  int num_logs = 0;

  std::map<uint64_t, std::pair<uint64_t, uint64_t>> rs2nums;

  fprintf(fout, "log size read_num write_num\n"); 
  fprintf(fout2, "size read_num write_num\n");

//  for (int i = 1; i < argc; i++) 
  for (int i = 1000; i <= 26075; i++)
  {
    char filename[200];
    sprintf(filename, "%d.data", i);
//    sprintf(filename, "%s", argv[i]);

    FILE* fin = fopen(filename, "r");
    if (fin == nullptr) continue;

    char s[200], logname[200];
    strcpy(s, filename);
    for (int j = 0; j < strlen(s); j++) {
      if (s[j] == '/' || s[j] == '.') {
        s[j] = '\0';
        break;
      }
    }
    strcpy(logname, s);
    ++num_logs;
    if (num_logs % 100 == 0) printf("%d: %s, \n", num_logs, logname);

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
  return 0;
}
