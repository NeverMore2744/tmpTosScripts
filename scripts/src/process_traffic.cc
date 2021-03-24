#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include <assert.h>
#include <set>
#include <map>
#include <vector>
#include <algorithm>
#include <cmath>

int d[10] = {1, 10, 100, 1000, 10000, 100000, 1000000};
int cp[10] = {100, 1000, 10000, 100000, 1000000, 10000000, 999999999};
int dt[10] = {1, 60, 600, 6000, 60000, 600000, 6000000, 60000000};
int cpt[10] = {600, 6000, 60000, 600000, 6000000, 60000000, 600000000, 999999999};
std::set<uint64_t> mins;
std::vector<int> minTimestamps;
std::vector<int> maxTimestamps;

std::map<int, std::set<int>> min2activeVolumes;
std::map<int, std::set<int>> readActive;
std::map<int, std::set<int>> writeActive;

std::set<int> readActiveSets;
std::set<int> writeActiveSets;

void readGroup(FILE* fin, std::vector<uint64_t>& data, std::vector<uint64_t>& perHourTraffic) {
  char s[200];
  uint64_t largestMin, timeInMin, value, rows;
  int ret;
  char* retChar;
  
  fgets(s, 200, fin);
  ret = sscanf(s, "%lld", &rows);
  assert(ret == 1);
//  printf("rows = %lld\n", rows);

  data.clear();
  perHourTraffic.clear();
  for (int i = 0; i < 24; i++) {
    perHourTraffic.push_back(0);
  }

  int index = -1;

  for (int i = 0; i < (int)rows; i++) 
  {
    retChar = fgets(s, 200, fin);
    if (retChar == NULL) break;
    ret = sscanf(s, "%llu%llu", &timeInMin, &value);
    assert(ret == 2);

    perHourTraffic[timeInMin / 60 % 24] += value;

    while (index < ((int)timeInMin - 1)) {
      data.push_back(0);
      index++;
    }
    data.push_back(value);
    index++;

    if (value > 0) {
      mins.insert(i);
    }
  }
}

void getPeakAvgStddev(int lognum, std::vector<uint64_t>& data, uint64_t& peak, double& avg, double& stddev) {
  peak = 0;
  avg = 0.0;
  stddev = 0.0;

  for (int i = 0; i < (int)data.size(); i++) {
    if (data[i] > 0) {
      avg += (double)data[i];
      if (data[i] > peak) peak = data[i];
    }
  }

  int endTime = maxTimestamps[lognum], startTime = minTimestamps[lognum];
  avg /= (double)(endTime - startTime + 1);

  for (int i = startTime; i <= endTime; i++) {
    stddev += pow((double)data[i] - avg, 2);
  }
  stddev = sqrt(stddev / (endTime - startTime + 1));
}

void combine(std::vector<uint64_t>& data, std::vector<uint64_t>& data1, std::vector<uint64_t>& data2) {
  data.clear();
  for (int i = 0; i < (int)std::min(data1.size(), data2.size()); i++) {
    data.push_back(data1[i] + data2[i]);
  }

  if (data1.size() > data2.size()) {
    for (int i = data2.size(); i < (int)data1.size(); i++) {
      data.push_back(data1[i]);
    }
  } else {
    for (int i = data1.size(); i < (int)data2.size(); i++) {
      data.push_back(data2[i]);
    }
  }
}

void outputPerHour(FILE* fout, const char* type, const char* log_name, std::vector<uint64_t>& perHourTraffic) {
  for (int i = 0; i < (int)perHourTraffic.size(); i++) {
    fprintf(fout, "%s %llu %s\n", log_name, perHourTraffic[i], type);
  }
}

int main(int argc, char** argv) {
  setbuf(stdout, NULL);

  FILE* fout = fopen("total_traffic.data", "w");
  FILE* fout2 = fopen("per_device_traffic.data", "w");
  FILE* fout3 = fopen("active_devices.data", "w");
  FILE* fout4 = fopen("interfered_minutes.data", "w");
  FILE* fout5 = fopen("active_days.data", "w");
  FILE* fout6 = fopen("per_device_per_hour_traffic.data", "w");

  fprintf(fout, "timeInMin RR WR RT WT UT\n"); 
  fprintf(fout2, "log peakRR peakWR peakRT peakWT peakUT avgRR avgWR avgRT avgWT avgUT stddevRR stddevWR stddevRT stddevWT stddevUT peakR peakT avgR avgT stddevR stddevT minTimeInMin maxTimeInMin activeTimeInMin readActiveTimeInMin writeActiveTimeInMin\n");
  fprintf(fout3, "timeInMin activeDevices readActive writeActive\n");
  fprintf(fout4, "log readIntensive writeIntensive timeInMin\n");
  fprintf(fout5, "log activeDays\n");

  // total in all devices
  std::map<uint64_t, uint64_t> min2readReqsTotal;
  std::map<uint64_t, uint64_t> min2writeReqsTotal;
  std::map<uint64_t, uint64_t> min2readTrafficTotal;
  std::map<uint64_t, uint64_t> min2writeTrafficTotal;
  std::map<uint64_t, uint64_t> min2updateTrafficTotal;

  std::vector<uint64_t> readReqs;
  std::vector<uint64_t> writeReqs;
  std::vector<uint64_t> reqs;
  std::vector<uint64_t> readTraffic;
  std::vector<uint64_t> writeTraffic;
  std::vector<uint64_t> updateTraffic;
  std::vector<uint64_t> traffic;

//  std::vector<uint64_t> perHourReadReqTotal;
//  std::vector<uint64_t> perHourWriteReqTotal;
//  std::vector<uint64_t> perHourReadTrafficTotal;
//  std::vector<uint64_t> perHourWriteTrafficTotal;
//  std::vector<uint64_t> perHourUpdateTrafficTotal;

//  for (int i = 1; i < argc; i++) 
  for (int fn = 1058, i = 0; fn <= 26075; fn++) 
  {
    char filename[200];
    sprintf(filename, "%d_traffic.data", fn);
//    sprintf(filename, "%s", argv[i]);

    FILE* fin = fopen(filename, "r");
    if (fin == nullptr) continue;
    i++;

    char s[200], logname[200];
    strcpy(s, filename);

    for (int j = 0; j < strlen(s); j++) { 
      if (s[j] == '/' || s[j] == '.') {
        s[j] = '\0';
        break;
      }
    }
    printf("log %d (%d): %s\n", fn, i, s);

    strcpy(logname, s);
    uint64_t peakRR, peakWR, peakRT, peakWT, peakUT;
    uint64_t peakR, peakT;
    double avgRR, avgWR, avgRT, avgWT, avgUT;
    double avgR, avgT;
    double stddevRR, stddevWR, stddevRT, stddevWT, stddevUT;
    double stddevR, stddevT;

    std::set<uint64_t> activeDays;
    std::vector<uint64_t> perHourRead, perHourWrite, perHourUpdate;

    // 1. requests
    readGroup(fin, readReqs, perHourRead);
    readGroup(fin, writeReqs, perHourWrite);
    combine(reqs, readReqs, writeReqs);

    outputPerHour(fout6, "RReq", logname, perHourRead);
    outputPerHour(fout6, "WReq", logname, perHourWrite);

    minTimestamps.push_back(0);
    maxTimestamps.push_back(0);
    int first = 1;
    for (int r = 0; r < (int)reqs.size(); r++) {
      if (reqs[r] > 0) {
        if (first) minTimestamps[i-1] = r, first = 0; 
        maxTimestamps[i-1] = r;

        min2activeVolumes[r/10*10].insert(i);
        if (readReqs[r] > 0) {
          readActive[r/10*10].insert(i);
          readActiveSets.insert(i);
        }
        if (writeReqs[r] > 0) {
          writeActive[r/10*10].insert(i);
          writeActiveSets.insert(i);
        }

        min2readReqsTotal[r] += readReqs[r];
        min2writeReqsTotal[r] += writeReqs[r];

        activeDays.insert(r / 1440);
      }
    }

    int activeTime = 0, readActiveTime = 0, writeActiveTime = 0;
    bool f1 = false, f2 = false, f3 = false;
    for (int r = 0; r < (int)reqs.size(); r++) {
      if (reqs[r] > 0) {
        if (!f1) activeTime += 10, f1 = true;
        if (!f2 && readReqs[r] > 0) readActiveTime += 10, f2 = true;
        if (!f3 && writeReqs[r] > 0) writeActiveTime += 10, f3 = true;
      }
      if (r % 10 == 9) f1 = f2 = f3 = false;
    }

    getPeakAvgStddev(i-1, readReqs, peakRR, avgRR, stddevRR);
    getPeakAvgStddev(i-1, writeReqs, peakWR, avgWR, stddevWR);
    getPeakAvgStddev(i-1, reqs, peakR, avgR, stddevR);

    readGroup(fin, readTraffic, perHourRead);
    readGroup(fin, writeTraffic, perHourWrite);
    readGroup(fin, updateTraffic, perHourUpdate);
    combine(traffic, readTraffic, writeTraffic);

    outputPerHour(fout6, "RBlock", logname, perHourRead);
    outputPerHour(fout6, "WBlock", logname, perHourWrite);
    outputPerHour(fout6, "UBlock", logname, perHourUpdate);

    getPeakAvgStddev(i-1, readTraffic, peakRT, avgRT, stddevRT);
    getPeakAvgStddev(i-1, writeTraffic, peakWT, avgWT, stddevWT);
    getPeakAvgStddev(i-1, updateTraffic, peakUT, avgUT, stddevUT);
    getPeakAvgStddev(i-1, traffic, peakT, avgT, stddevT);

    fprintf(fout2, "%s %llu %llu %llu %llu %llu %.2lf %.2lf %.2lf %.2lf %.2lf %.2lf %.2lf %.2lf %.2lf %.2lf %llu %llu %.2lf %.2lf %.2lf %.2lf %d %d %d %d %d\n", logname,
       peakRR, peakWR, peakRT, peakWT, peakUT, 
       avgRR, avgWR, avgRT, avgWT, avgUT,
       stddevRR, stddevWR, stddevRT, stddevWT, stddevUT,
       peakR, peakT, avgR, avgT, stddevR, stddevT,
       minTimestamps[i-1], maxTimestamps[i-1],
       activeTime, readActiveTime, writeActiveTime);

    int sum = 0; 
    int rsum = 0, wsum = 0;
    for (int r = 0; r < (int)reqs.size(); r++) {
      if ((double)readReqs[r] > avgRR + 3 * stddevRR &&
          (double)writeReqs[r] > avgWR + 3 * stddevWR) {
        sum++;
      }
      if ((double)readReqs[r] > avgRR + 3 * stddevRR) rsum++; 
      if ((double)writeReqs[r] > avgWR + 3 * stddevWR) wsum++; 

      if (readTraffic[r] > 0) {
        min2readTrafficTotal[r] += readTraffic[r];
      }
      if (writeTraffic[r] > 0) {
        min2writeTrafficTotal[r] += writeTraffic[r];
      }
      if (updateTraffic[r] > 0) {
        min2updateTrafficTotal[r] += updateTraffic[r];
      }
    }

    fprintf(fout4, "%s %d %d %d\n", logname, rsum, wsum, sum);
    fprintf(fout5, "%s %d\n", logname, (int)activeDays.size());
    fclose(fin);
  }

  for (auto& it : mins) {
    fprintf(fout, "%lld %lld %lld %lld %lld %lld\n", it, 
        min2readReqsTotal[it],
        min2writeReqsTotal[it],
        min2readTrafficTotal[it],
        min2writeTrafficTotal[it],
        min2updateTrafficTotal[it]);
  }

  for (auto& it : min2activeVolumes) {
    fprintf(fout3, "%d %d %d %d\n", it.first, (int)(it.second.size()), 
        (int)(readActive[it.first].size()), (int)(writeActive[it.first].size()));
  }
  printf("read set:%d\n", (int)readActiveSets.size());
  printf("write set:%d\n", (int)writeActiveSets.size());

  fclose(fout);
  fclose(fout2);
  fclose(fout3);
  fclose(fout4);
  fclose(fout5);
  return 0;
}
