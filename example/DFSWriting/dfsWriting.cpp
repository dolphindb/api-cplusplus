#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>

using namespace dolphindb;
using namespace std;

void ShowUsage() {
  cout << "DolpinDB DFS writing demo" << endl;
  cout << "Usage example:--h=127.0.0.1 --p=8921 --c=1000 --n=5 --s= " << endl;
  cout << "Options :" << endl;
  cout << " --h=127.0.0.1 Mandatory,dolphindb host" << endl;
  cout << " --p=8921 Mandatory,dolphindb port" << endl;
  cout << " --c=1000 Mandatory,The number of records inserted per batch"
       << endl;
  cout << " --n=5 Optional,batches,default is 1" << endl;
  cout << " --s=1574380800 Optional,start time,default is now:"
       << Util::getEpochTime() / 1000 << endl;
  cout << " --help Print this help." << endl;
  return;
}

TableSP createDemoTable1(long rows, long startTime, int timeInc) {
  vector<string> colNames = {"timestamp",      "areaId",
                             "deviceId",       "onlineStatus",
                             "offlineReason",  "workStatus",
                             "signalStatus",   "loginStatus",
                             "detected",       "onlineStatusChangeTime",
                             "devCurrentTime", "ntpTime",
                             "clockDeviation", "clockStatusChangeTime",
                             "clockStatus",    "statusErrorCode"};
  vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_UUID,  DT_UUID,  DT_SHORT,
                                DT_SHORT,     DT_SHORT, DT_SHORT, DT_SHORT,
                                DT_SHORT,     DT_LONG,  DT_LONG,  DT_LONG,
                                DT_INT,       DT_LONG,  DT_SHORT, DT_SYMBOL};
  int colNum = 16, rowNum = rows, indexCapacity = rows;
  ConstantSP table =
      Util::createTable(colNames, colTypes, rowNum, indexCapacity);
  vector<VectorSP> columnVecs;
  for (int i = 0; i < colNum; i++)
    columnVecs.push_back(table->getColumn(i));

  for (int i = 0; i < rowNum; i++) {
    columnVecs[0]->setLong(i, startTime + timeInc * i);
    unsigned char data[16] = {1, 2,  3,  4,  5,  6,  7,  8,
                              9, 10, 11, 12, 13, 14, 15, (unsigned char)i};
    columnVecs[1]->setBinary(i, 16, data);
    columnVecs[2]->setBinary(i, 16, data);
    columnVecs[3]->setShort(i, 1 * i);

    columnVecs[4]->setShort(i, 2 * i);
    columnVecs[5]->setShort(i, 3 * i);
    columnVecs[6]->setShort(i, 4 * i);
    columnVecs[7]->setShort(i, 5 * i);
    columnVecs[8]->setShort(i, 6 * i);
    columnVecs[9]->setLong(i, 7 * i);
    columnVecs[10]->setLong(i, 8 * i);
    columnVecs[11]->setLong(i, 9 * i);
    columnVecs[12]->setInt(i, 9 * i);
    columnVecs[13]->setLong(i, 10 * i);
    columnVecs[14]->setShort(i, 11 * i);
    columnVecs[15]->setString(i, "just a demo");
  }
  return table;
}
TableSP createDemoTable2(long rows, long startTime, int timeInc) {

  vector<string> colNames = {"timestamp",      "areaId",
                             "deviceId",       "onlineStatus",
                             "offlineReason",  "workStatus",
                             "signalStatus",   "loginStatus",
                             "detected",       "onlineStatusChangeTime",
                             "devCurrentTime", "ntpTime",
                             "clockDeviation", "clockStatusChangeTime",
                             "clockStatus",    "statusErrorCode"};
  vector<DATA_TYPE> colTypes = {DT_TIMESTAMP, DT_UUID,  DT_UUID,  DT_SHORT,
                                DT_SHORT,     DT_SHORT, DT_SHORT, DT_SHORT,
                                DT_SHORT,     DT_LONG,  DT_LONG,  DT_LONG,
                                DT_INT,       DT_LONG,  DT_SHORT, DT_SYMBOL};
  int colNum = 16, rowNum = rows, indexCapacity = rows;
  ConstantSP table =
      Util::createTable(colNames, colTypes, rowNum, indexCapacity);
  vector<VectorSP> columnVecs;
  for (int i = 0; i < colNum; i++)
    columnVecs.push_back(table->getColumn(i));

  long long timestampBuf[Util::BUF_SIZE];
  short onlineStatusBuf[Util::BUF_SIZE];
  short offlineReasonBuf[Util::BUF_SIZE];
  short workStatusBuf[Util::BUF_SIZE];
  short signalStatusBuf[Util::BUF_SIZE];
  short loginStatusBuf[Util::BUF_SIZE];
  short detectedBuf[Util::BUF_SIZE];
  long long onlineStatusChangeTimeBuf[Util::BUF_SIZE];
  long long devCurrentTimeTimeBuf[Util::BUF_SIZE];
  long long ntpTimeBuf[Util::BUF_SIZE];
  int clockDeviationBuf[Util::BUF_SIZE];
  long long clockStatusChangeTimeBuf[Util::BUF_SIZE];
  short clockStatusBuf[Util::BUF_SIZE];

  int start = 0;
  unsigned char data[16] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
  while (start < rowNum) {
    size_t len = std::min(Util::BUF_SIZE, rowNum - start);
    long long *timestamp =
        columnVecs[0]->getLongBuffer(start, len, timestampBuf);
    short *onlineStatus =
        columnVecs[3]->getShortBuffer(start, len, onlineStatusBuf);
    short *offlineReason =
        columnVecs[4]->getShortBuffer(start, len, offlineReasonBuf);
    short *workStatus =
        columnVecs[5]->getShortBuffer(start, len, workStatusBuf);
    short *signalStatus =
        columnVecs[6]->getShortBuffer(start, len, signalStatusBuf);
    short *loginStatus =
        columnVecs[7]->getShortBuffer(start, len, loginStatusBuf);
    short *detected = columnVecs[8]->getShortBuffer(start, len, detectedBuf);
    long long *onlineStatusChangeTime =
        columnVecs[9]->getLongBuffer(start, len, onlineStatusChangeTimeBuf);
    long long *devCurrentTimeTime =
        columnVecs[10]->getLongBuffer(start, len, devCurrentTimeTimeBuf);
    long long *ntpTime = columnVecs[11]->getLongBuffer(start, len, ntpTimeBuf);
    int *clockDeviation =
        columnVecs[12]->getIntBuffer(start, len, clockDeviationBuf);
    long long *clockStatusChangeTime =
        columnVecs[13]->getLongBuffer(start, len, clockStatusChangeTimeBuf);
    short *clockStatus =
        columnVecs[14]->getShortBuffer(start, len, clockStatusBuf);
    for (int i = 0; i < (int)len; ++i) {
      timestamp[i] = startTime + timeInc * (i + start);
      data[15] = (unsigned char)(i + start);
      columnVecs[1]->setBinary(i + start, 16, data);
      columnVecs[2]->setBinary(i + start, 16, data);

      onlineStatus[i] = start + i;
      offlineReason[i] = (start + i) * 2;
      workStatus[i] = (start + i) * 3;
      signalStatus[i] = (start + i) * 4;
      loginStatus[i] = (start + i) * 5;
      detected[i] = (start + i) * 6;
      onlineStatusChangeTime[i] = (start + i) * 7;
      devCurrentTimeTime[i] = (start + i) * 8;
      ntpTime[i] = (start + i) * 9;
      clockDeviation[i] = (start + i) * 10;
      clockStatusChangeTime[i] = (start + i) * 11;
      clockStatus[i] = (start + i) * 2;

      columnVecs[15]->setString(i + start,
                                "just a demo" + std::to_string(i + start));
    }
    columnVecs[0]->setLong(start, len, timestamp);
    columnVecs[3]->setShort(start, len, onlineStatus);
    columnVecs[4]->setShort(start, len, offlineReason);
    columnVecs[5]->setShort(start, len, workStatus);
    columnVecs[6]->setShort(start, len, signalStatus);
    columnVecs[7]->setShort(start, len, loginStatus);
    columnVecs[8]->setShort(start, len, detected);
    columnVecs[9]->setLong(start, len, onlineStatusChangeTime);
    columnVecs[10]->setLong(start, len, devCurrentTimeTime);
    columnVecs[11]->setLong(start, len, ntpTime);
    columnVecs[12]->setInt(start, len, clockDeviation);
    columnVecs[13]->setLong(start, len, clockStatusChangeTime);
    columnVecs[14]->setShort(start, len, clockStatus);
    start += len;
  }
  return table;
}

int main(int argc, char *argv[]) {

  if (argc < 2) {
    cout << "No arguments, you MUST give an argument at least!" << endl;
    ShowUsage();
    return -1;
  }

  int nOptionIndex = 1;
  string cString, nString, hString, pString, sString;
  stringstream cSS, nSS, pSS, sSS;
  long cLong, nLong, pLong, sLong;

  while (nOptionIndex < argc) {

    if (strncmp(argv[nOptionIndex], "--c=", 4) == 0) { // get records number
      cString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--h=", 4) == 0) { // get host
      hString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--p=", 4) == 0) { // get port
      pString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--n=", 4) == 0) { // get batches
      nString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--s=", 4) == 0) { // get start time
      sString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--help", 6) == 0) { // help
      ShowUsage();
      return 0;
    } else {
      cout << "Options '" << argv[nOptionIndex] << "' not valid. Run '"
           << argv[0] << "' for details." << endl;
      return -1;
    }
    nOptionIndex++;
  }

  if (cString.empty()) {
    cout << "--c is required" << endl;
    ShowUsage();
    return -1;
  } else {
    cSS << cString;
    cSS >> cLong;
  }
  if (pString.empty()) {
    cout << "--p is required" << endl;
    ShowUsage();
    return -1;
  } else {
    pSS << pString;
    pSS >> pLong;
  }
  if (hString.empty()) {
    cout << "--h is required" << endl;
    ShowUsage();
    return -1;
  }
  if (nString.empty()) {
    nLong = 1;
  } else {
    nSS << nString;
    nSS >> nLong;
  }

  if (sString.empty()) {
    sLong = Util::getEpochTime(); // 1574380800;
    cout << "starttime=" << sLong << endl;
  } else {
    sSS << sString;
    sSS >> sLong;
  }
  DBConnection conn;
  try {
    bool ret = conn.connect(hString, pLong, "admin", "123456");
    if (!ret) {
      cout << "Failed to connect to the server" << endl;
      return 0;
    }
  } catch (exception &ex) {
    cout << "Failed to  connect  with error: " << ex.what();
    return -1;
  }
  cout << "Please waiting..." << endl;

  TableSP table = createDemoTable2(cLong, sLong, 1);
  long long startTime = Util::getEpochTime();
  for (unsigned int i = 0; i < nLong; i++) {
    vector<ConstantSP> args;
    args.push_back(table);
    conn.run("tableInsert{loadTable('dfs://DolphinDB', `device_status)}", args);
  }
  long long endTime = Util::getEpochTime();
  printf("Insert %ld rows %ld times, used %lld ms.\n", cLong, nLong,
         endTime - startTime);
  return 0;
}
