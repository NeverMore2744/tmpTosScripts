#include<fstream>
#include<iostream>
#include<chrono>
#include<sys/time.h>
#include<utility>

typedef std::chrono::high_resolution_clock Clock;
typedef std::chrono::milliseconds milliseconds;

class Trace {
  std::unordered_map<std::string, std::pair<uint64_t, uint64_t>> properties;
  std::string line_, volumeId_;

  public:
  void loadProperty(char *propertyFile, const char* selectedVolume) {
    FILE *f = nullptr; 
    uint64_t uniqueLba, maxLba;
    std::string volumeId;
    volumeId_ = std::string(selectedVolume);

    if (propertyFile != nullptr) {
      f = fopen(propertyFile, "r");
      char tmp[300];
      while (fscanf(f, "%s %lu %lu", tmp, &uniqueLba, &maxLba) != EOF) {
        volumeId = std::string(tmp);
        properties[volumeId] = std::make_pair(uniqueLba, maxLba);
      }
    }
  }

  void myTimer(bool start, const char* event = "") {
    static Clock::time_point ts;
    static uint64_t cnt = 0;
    if (start) {
      ts = Clock::now();
      cnt = 0;
    } else {
      if (++cnt % 1000000 == 0) {
        Clock::time_point te = Clock::now();
        double duration2 = (std::chrono::duration_cast <std::chrono::milliseconds> (te - ts)).count() / 1024.0;
        fprintf(stderr, "Volume %s analysis on %s: %lu requests, %lf seconds\n", 
            volumeId_.c_str(), event, cnt, duration2);
      }
    }
  }

  uint64_t getUniqueLba(std::string volumeId) {
    return properties[volumeId].first;
  }

  uint64_t getMaxLba(std::string volumeId) {
    return properties[volumeId].second;
  }

//  int readNextRequest(FILE *f, uint64_t &timestamp, char &type, uint64_t &offset, uint64_t &length) {
//#ifdef MSRC_TRACE
//    char volume[100], typeStr[4];
//    uint64_t volumeNum, latency;
//
//    if (fscanf(f, "%lu,%[^,],%lu,%[^,],%lu,%lu,%lu", &timestamp, volume,
//          &volumeNum, typeStr, &offset, &length, &latency) == EOF) return -1;
//    type = typeStr[0];
//#elif CLOUD_TRACE
//    uint64_t volumeId;
//    int t;
//    if (t = (fscanf(f, "%I64u,%c,%I64u,%I64u,%I64u", &volumeId, &type, &offset, &length, &timestamp)) == EOF) {
//      return -1;
//    }
//#endif
//    return 0;
//  }


  // timestamp: in 10e-7 second
  // offset and length: in 4KiB ("sector" enabled: 512B)
  int readNextRequestFstream(std::istream& is, uint64_t &timestamp, bool &isWrite, uint64_t &offset, uint64_t &length, char* line_cstr, bool sector = false) {
    char type;
    if (!std::getline(is, line_)) {
      return 0;
    }
    strcpy(line_cstr, line_.c_str());

#ifdef TENCENTCLOUD
    uint64_t beginTimestampInSec = 1538326799llu; // Minimum timestamp in Tencent 

    int pos = strlen(line_cstr) - 1;
    for ( ; pos >= 0 && line_cstr[pos] != ','; pos--); pos++; 
    int pos2 = pos-2;
    for ( ; pos2 >= 0 && line_cstr[pos2] != ','; pos2--); pos2++;
    int pos3 = pos2-2;
    for ( ; pos3 >= 0 && line_cstr[pos3] != ','; pos3--); pos3++;

    offset = 0, length = 0, timestamp = 0;
    isWrite = line_cstr[pos] != '0';  // 1 == Write
    for (int i = pos2; isdigit(line_cstr[i]); i++) length = length*10 + (line_cstr[i] - '0');
    for (int i = pos3; isdigit(line_cstr[i]); i++) offset = offset*10 + (line_cstr[i] - '0');
    for (int i = 0; isdigit(line_cstr[i]); i++) timestamp = timestamp*10 + (line_cstr[i] - '0');

    timestamp = (timestamp - beginTimestampInSec) * 10000000;

    if (!sector) {
      length = (offset + length + 7) / 8 - offset / 8;
      offset /= 8;
    }
    return 1;

#elif ALICLOUD 
    uint64_t beginTimestampInUsec = 1577808000000000llu;
    int pos = strlen(line_cstr) - 1;
    for ( ; pos >= 0 && line_cstr[pos] != ','; pos--); pos++; 
    int pos2 = pos-2;
    for ( ; pos2 >= 0 && line_cstr[pos2] != ','; pos2--); pos2++;
    int pos3 = pos2-2;
    for ( ; pos3 >= 0 && line_cstr[pos3] != ','; pos3--); pos3++;

    offset = 0, length = 0, timestamp = 0;
    isWrite = (line_cstr[pos3-2] == 'w' || line_cstr[pos3-2] == 'W');
    for (int i = pos; isdigit(line_cstr[i]); i++) timestamp = timestamp*10 + (line_cstr[i] - '0');
    for (int i = pos2; isdigit(line_cstr[i]); i++) length = length*10 + (line_cstr[i] - '0');
    for (int i = pos3; isdigit(line_cstr[i]); i++) offset = offset*10 + (line_cstr[i] - '0');

    timestamp = (timestamp - beginTimestampInUsec) * 10; // Change to 100 ns
    if (!sector) {
      length = (offset + length + 4095) / 4096 - offset / 4096;
      offset /= 4096;
    } else {
      length = (offset + length + 511) / 512 - offset / 512;
      offset /= 512;
    }
    return 1;

#elif MSRC 
    uint64_t beginTimestampIn100ns = 128166372000000000llu; // Minimum timestamp in MSR 
    char volume[100], typeStr[4];
    uint64_t volumeNum, latency;

    sscanf(line_cstr, "%lu,%[^,],%lu,%[^,],%lu,%lu,%lu", &timestamp, volume,
          &volumeNum, typeStr, &offset, &length, &latency);
    timestamp -= beginTimestampIn100ns;
    if (timestamp / 10000000 / 60 >= 7 * 24 * 60) return 0; // only take 7 days

    isWrite = typeStr[0] == 'W';
    if (!sector) {
      length = (offset + length + 4095) / 4096 - offset / 4096;
      offset /= 4096;
    } else {
      length = (offset + length + 511) / 512 - offset / 512;
      offset /= 512;
    }
    return 1;

#elif SSDTRACE 
    uint64_t beginTimestampInNsec = 0;
    uint64_t volumeId;
    int pos = strlen(line_cstr) - 3;  // Jump over "success"
    for ( ; pos >= 0 && line_cstr[pos] != ' '; pos--); pos++; 
    pos = pos - 2;  // Jump over "elapsed"
    for ( ; pos >= 0 && line_cstr[pos] != ' '; pos--); pos++; // 4th: timestamp
    int pos2 = pos-2;
    for ( ; pos2 >= 0 && line_cstr[pos2] != ' '; pos2--); pos2++; // 3th: length
    int pos3 = pos2-2;
    for ( ; pos3 >= 0 && line_cstr[pos3] != ' '; pos3--); pos3++; // 2th: offset 

    offset = 0, length = 0, timestamp = 0;
    isWrite = line_cstr[0] == 'W';
    type = line_cstr[0];
    for (int i = pos; isdigit(line_cstr[i]); i++) timestamp = timestamp*10 + (line_cstr[i] - '0');
    for (int i = pos2; isdigit(line_cstr[i]); i++) length = length*10 + (line_cstr[i] - '0');
    for (int i = pos3; isdigit(line_cstr[i]); i++) offset = offset*10 + (line_cstr[i] - '0'); // unit: 512B

    if (!sector) {
      length = (offset + length + 7) / 8 - offset / 8;  // Change to unit: 4 kib blocks
      offset /= 8;
    } 
    return 1;

#endif
    return 0;
  }
};
