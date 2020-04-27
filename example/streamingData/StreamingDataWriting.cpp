#include "Concurrent.h"
#include "DolphinDB.h"
#include "Util.h"
#include <arpa/inet.h>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>
#include <thread>

using namespace dolphindb;
using namespace std;
#define MAX_THREAD_NUM 50
DBConnection conn[MAX_THREAD_NUM];
SmartPointer<BoundedBlockingQueue<TableSP>> tableQueue[MAX_THREAD_NUM];

struct parameter {
  int index;
  int count;
  long cLong;
  long nLong;
  long nTime;
};

void showUsage() {
  cout << "DolpinDB Multi-threaded streaming date writing program" << endl;
  cout << "Usage example:--h=127.0.0.1 --p=8921 --c=1000 --n=5 --t=5 " << endl;
  cout << "Options :" << endl;
  cout << " --h=127.0.0.1 Mandatory,dolphindb host，Multiple hosts separated "
          "by commas"
       << endl;
  cout << " --p=8921 Mandatory,dolphindb port，Multiple ports separated by "
          "commas.The number of ports should be the same of hosts!"
       << endl;
  cout << " --c=1000 Mandatory,The number of records inserted per thread"
       << endl;
  cout << " --n=5 Optional,Batches insertions per thread,default is 1" << endl;
  cout << " --t=5 Optional,Threads number,default is 1,max is "
       << MAX_THREAD_NUM << endl;
  cout << " --help Print this help." << endl;
  return;
}

TableSP createDemoTable(long rows) {
  vector<string> colNames = {
      "id",      "cbool",     "cchar",      "cshort",    "cint",
      "clong",   "cdate",     "cmonth",     "ctime",     "cminute",
      "csecond", "cdatetime", "ctimestamp", "cnanotime", "cnanotimestamp",
      "cfloat",  "cdouble",   "csymbol",    "cstring",   "cuuid",
      "cip",     "cint128"};

  vector<DATA_TYPE> colTypes = {
      DT_LONG,   DT_BOOL,     DT_CHAR,      DT_SHORT,    DT_INT,
      DT_LONG,   DT_DATE,     DT_MONTH,     DT_TIME,     DT_MINUTE,
      DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME, DT_NANOTIMESTAMP,
      DT_FLOAT,  DT_DOUBLE,   DT_SYMBOL,    DT_STRING,   DT_UUID,
      DT_IP,     DT_INT128};
  int colNum = 22, rowNum = rows, indexCapacity = rows;
  ConstantSP table =
      Util::createTable(colNames, colTypes, rowNum, indexCapacity);
  vector<VectorSP> columnVecs;
  for (int i = 0; i < colNum; i++)
    columnVecs.push_back(table->getColumn(i));
  unsigned char ip[16] = {0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14};
  for (int i = 0; i < rowNum; i++) {
    columnVecs[0]->setLong(i, i);
    columnVecs[1]->setBool(i, i % 2);
    columnVecs[2]->setChar(i, i);
    columnVecs[3]->setShort(i, i);
    columnVecs[4]->setInt(i, i);
    columnVecs[5]->setLong(i, i);
    columnVecs[6]->set(i, Util::parseConstant(DT_DATE, "2020.01.01"));
    columnVecs[7]->setInt(i, 24240); // 2020.01M
    columnVecs[8]->setInt(i, i);
    columnVecs[9]->setInt(i, i);
    columnVecs[10]->setInt(i, i);
    columnVecs[11]->setInt(i, 1577836800 + i);      // 2020.01.01 00:00:00+i
    columnVecs[12]->setLong(i, 1577836800000l + i); // 2020.01.01 00:00:00+i
    columnVecs[13]->setLong(i, i);
    columnVecs[14]->setLong(i, 1577836800000000000l +i); // 2020.01.01 00:00:00.000000000+i
    columnVecs[15]->setFloat(i, i);
    columnVecs[16]->setDouble(i, i);
    columnVecs[17]->setString(i, "sym" + to_string(i));
    columnVecs[18]->setString(i, "abc" + to_string(i));
    ip[15] = i;
    columnVecs[19]->setBinary(i, 16, ip);
    columnVecs[20]->setBinary(i, 16, ip);
    columnVecs[21]->setBinary(i, 16, ip);
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
    conn[pParam->index].run("tableInsert{objByName(`st1)}", args);
    pParam->nTime += Util::getEpochTime() - startTime;
  }
  printf("Thread %d,insert %ld rows %ld times, used %ld ms.\n", pParam->index,
         pParam->cLong, pParam->nLong, pParam->nTime);
  return NULL;
}
void *genData(void *arg) {
  struct parameter *pParam;
  pParam = (struct parameter *)arg;

  for (unsigned int i = 0; i < pParam->nLong; i++) {
    TableSP table = createDemoTable(pParam->cLong);
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
  stringstream cSS, nSS, pSS, tSS;
  long cLong, nLong, pLong, tLong;
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
       << " rows, took a total of  " + std::to_string(endTime - startTime) +
              " ms.  "
       << rowCount / (endTime - startTime) * 1000 / 10000 << " w/s " << endl;
  long timeSum = arg[0].nTime;
  for (int i = 1; i < tLong; ++i) {
    timeSum += arg[i].nTime;
  }
  cout << "Total time minus data preparation time:  "
       << std::to_string(timeSum / (double)tLong) + " ms" << endl;
  return 0;
}
