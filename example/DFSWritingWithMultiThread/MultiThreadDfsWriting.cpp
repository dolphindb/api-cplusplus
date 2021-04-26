#include "Concurrent.h"
#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <thread>
#include <arpa/inet.h>

using namespace dolphindb;
using namespace std;
#define BUCKETS 50
#define MAX_THREAD_NUM BUCKETS
DBConnection conn[MAX_THREAD_NUM];
SmartPointer<BoundedBlockingQueue<TableSP>> tableQueue[MAX_THREAD_NUM];

struct parameter {
  int index;
  int count;
  long cLong;
  long nLong;
  long nTime;
  long nStarttime;
};

void showUsage() {
  cout << "DolpinDB Multi-threaded performance test program" << endl;
  cout << "Usage example:--h=127.0.0.1 --p=8921 --c=1000 --n=5 --t=5 "
          "--s=1579080800000"
       << endl;
  cout << "Options :" << endl;
  cout << " --h=127.0.0.1 Mandatory,dolphindb host，Multiple hosts separated "
          "by commas"
       << endl;
  cout << " --p=8921 Mandatory,dolphindb port，Multiple ports separated by "
          "commas.The number of ports should be the same of hosts!"
       << endl;
  cout << " --c=1000 Mandatory,The number of records inserted per thread"
       << endl;
  cout << " --n=5 Optional,Batches  insertions per thread,default is 1" << endl;
  cout << " --t=5 Optional,Threads number,default is 1,max is " << BUCKETS
       << endl;
  cout << " --s=1574380800 Optional,start time,default is "
       << Util::getEpochTime() / 1000 << endl;
  cout << " --help Print this help." << endl;
  return;
}

TableSP createDemoTable(long rows, long startPartition, long partitionCount,
                        long startTime, int timeInc) {
  vector<string> colNames = {"fwname",
                             "filename",
                             "source_address",
                             "source_port",
                             "destination_address",
                             "destination_port",
                             "nat_source_address",
                             "nat_source_port",
                             "starttime",
                             "stoptime",
                             "elapsed_time"};
  vector<DATA_TYPE> colTypes = {DT_SYMBOL,   DT_STRING,   DT_IP, DT_INT,
                                DT_IP,       DT_INT,      DT_IP, DT_INT,
                                DT_DATETIME, DT_DATETIME, DT_INT};
  int colNum = 11, rowNum = rows, indexCapacity = rows;
  ConstantSP table =
      Util::createTable(colNames, colTypes, rowNum, indexCapacity);
  vector<VectorSP> columnVecs;
  for (int i = 0; i < colNum; i++)
    columnVecs.push_back(table->getColumn(i));

  unsigned char sip[16] = {0};
  sip[3] = 192;
  sip[2] = startPartition;
  sip[1] = partitionCount;
  ConstantSP spIP = Util::createConstant(DT_IP);
  for (int j = 1; j < 255; j++) {
    sip[0] = j;
    spIP->setBinary(0, 16, sip);
    if (spIP->getHash(BUCKETS) >= startPartition &&
        spIP->getHash(BUCKETS) < startPartition + partitionCount) {
      break;
    }
  }

  unsigned char ip[16] = {0};
  for (int i = 0; i < rowNum; i++) {
    columnVecs[0]->setString(i, "10.189.45.2:9000");
    columnVecs[1]->setString(i, std::to_string(startPartition)); 
    columnVecs[2]->setBinary(i, 16, sip);
    columnVecs[3]->setInt(i, 1 * i);
    memcpy(ip, (unsigned char *)&i, 4);
    columnVecs[4]->setBinary(i, 16, ip);
    columnVecs[5]->setInt(i, 2 * i);
    columnVecs[6]->set(i, Util::parseConstant(DT_IP, "192.168.1.1"));
    columnVecs[7]->setInt(i, 3 * i);
    columnVecs[8]->setLong(i, startTime + timeInc);
    columnVecs[9]->setLong(i, i + startTime + 100);
    columnVecs[10]->setInt(i, i);
  }
  return table;
}

void *writeData(void *arg) {
  struct parameter *pParam;
  pParam = (struct parameter *)arg;

  TableSP table;
  for (unsigned int i = 0; i < pParam->nLong; i++) {
    tableQueue[pParam->index]->pop(table);
    long long startTime = Util::getEpochTime();
    vector<ConstantSP> args;
    args.push_back(table);
    conn[pParam->index].run(
        "tableInsert{loadTable('dfs://natlog', `natlogrecords)}", args);
    pParam->nTime += Util::getEpochTime() - startTime;
  }
  printf("Thread %d,insert %ld rows %ld times, used %ld ms.\n", pParam->index,
         pParam->cLong, pParam->nLong, pParam->nTime);
  return NULL;
}
void *genData(void *arg) {
  struct parameter *pParam;
  pParam = (struct parameter *)arg;
  long partitionCount = BUCKETS / pParam->count;

  for (unsigned int i = 0; i < pParam->nLong; i++) {
    TableSP table =
        createDemoTable(pParam->cLong, partitionCount * pParam->index,
                        partitionCount, pParam->nStarttime, i * 5);
    tableQueue[pParam->index]->push(table);
  }
  return NULL;
}

int main(int argc, char *argv[]) {

  if (argc < 2) {
    cout << "No arguments, you MUST give an argument at least!" << endl;
    showUsage();
    return -1;
  }

  int nOptionIndex = 1;
  string cString, nString, hString, pString, tString, sString;
  stringstream cSS, nSS, pSS, tSS, sSS;
  long cLong, nLong, pLong, tLong, sLong;
  vector<string> vHost, vPort;

  while (nOptionIndex < argc) {

    if (strncmp(argv[nOptionIndex], "--c=", 4) == 0) { // get records number per threads
      cString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--h=", 4) == 0) { // get host
      hString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--p=", 4) == 0) { // get port
      pString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--n=", 4) == 0) { // get batches
      nString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--t=", 4) == 0) { // get thread
      tString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--s=", 4) == 0) { // get start time
      sString = &argv[nOptionIndex][4];
    } else if (strncmp(argv[nOptionIndex], "--help", 6) == 0) { // help
      showUsage();
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
    showUsage();
    return -1;
  } else {
    cSS << cString;
    cSS >> cLong;
  }
  if (pString.empty()) {
    cout << "--p is required" << endl;
    showUsage();
    return -1;
  } else {
    vPort = Util::split(pString, ',');
  }
  if (hString.empty()) {
    cout << "--h is required" << endl;
    showUsage();
    return -1;
  } else {
    vHost = Util::split(hString, ',');
  }
  if (nString.empty()) {
    nLong = 1;
  } else {
    nSS << nString;
    nSS >> nLong;
  }
  if (tString.empty()) {
    tLong = 1;
  } else {
    tSS << tString;
    tSS >> tLong;
  }
  if (sString.empty()) {
    sLong = Util::getEpochTime() / 1000; // 1574380800;
    cout << "starttime=" << sLong << endl;
  } else {
    sSS << sString;
    sSS >> sLong;
  }
  if (tLong > BUCKETS) {
    cout << "The number of threads must be less than " << BUCKETS << endl;
    showUsage();
    return -1;
  }

  if (vHost.size() != vPort.size()) {
    cout << "The number of host and port must be the same! " << vHost.size()
         << ":" << vPort.size() << endl;
    showUsage();
    return -1;
  }
  try {

    for (int i = 0; i < tLong; ++i) {
      hString = vHost[i % vHost.size()];
      pLong = std::stol(vPort[i % vPort.size()]);
      bool ret = conn[i].connect(hString, pLong, "admin", "123456");
      if (!ret) {
        cout << "Failed to connect to the server" << endl;
        return 0;
      }
      tableQueue[i] = new BoundedBlockingQueue<TableSP>(2);
    }
  } catch (exception &ex) {
    cout << "Failed to  connect  with error: " << ex.what();
    return -1;
  }
  cout << "Please waiting..." << endl;

  long long startTime = Util::getEpochTime();
  struct parameter arg[tLong];
  std::thread genThreads[tLong];
  std::thread writeThreads[tLong];
  for (int i = 0; i < tLong; ++i) {
    arg[i].index = i;
    arg[i].count = tLong;
    arg[i].nLong = nLong;
    arg[i].cLong = cLong;
    arg[i].nTime = 0;
    arg[i].nStarttime = sLong;
    genThreads[i] = std::thread(genData, (void *)&arg[i]);
    writeThreads[i] = std::thread(writeData, (void *)&arg[i]);
  }

  for (int i = 0; i < tLong; ++i) {
    genThreads[i].join();
    writeThreads[i].join();
  }
  long long endTime = Util::getEpochTime();
  long long rowCount = cLong * nLong * tLong;
  cout << "Inserted " << rowCount
       << " rows, took a total of  " + std::to_string(endTime - startTime) + " ms.  "
       << rowCount / (endTime - startTime) * 1000 / 10000 << " w/s " << endl;
  long timeSum = arg[0].nTime;
  for (int i = 1; i < tLong; ++i) {
    timeSum += arg[i].nTime;
  }
  cout << "Total time minus data preparation time:  "
       << std::to_string(timeSum / (double)tLong) + " ms" << endl;
  return 0;
}
