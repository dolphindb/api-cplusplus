#include "DolphinDB.h"
#include "Util.h"
#include <iostream>
#include <sstream>
#include <string>
#include <sys/time.h>
//#include <string.h>

using namespace dolphindb;
using namespace std;

TableSP createDemoTable(long rows) {
    vector < string > colNames = { "cbool", "cchar", "cshort", "cint", "clong", "cdate", "cmonth", "ctime", "cminute", "csecond", "cdatetime", "ctimestamp", "cnanotime",
            "cnanotimestamp", "cfloat", "cdouble", "csymbol", "cstring", "cuuid", "cip", "cint128" };
    vector<DATA_TYPE> colTypes = { DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME,
            DT_NANOTIMESTAMP, DT_FLOAT, DT_DOUBLE, DT_SYMBOL, DT_STRING, DT_UUID, DT_IP, DT_INT128 };
    int colNum = 21, rowNum = rows, indexCapacity = rows;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    vector<VectorSP> columnVecs;
    for (int i = 0; i < colNum; i++)
        columnVecs.push_back(table->getColumn(i));
    unsigned char data[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };
    for (int i = 0; i < rowNum; i++) {
        columnVecs[0]->setBool(i, i % 2);
        columnVecs[1]->setChar(i, i * 2);
        columnVecs[2]->setShort(i, i * 3);
        columnVecs[3]->setInt(i, i * 4);
        columnVecs[4]->setLong(i, i * 5);
        columnVecs[5]->setInt(i, 18262); //set(i, Util::parseConstant(DT_DATE, "2020.01.01"));
        columnVecs[6]->setInt(i, 24240); // 2020.01M
        columnVecs[7]->setInt(i, i * 7);
        columnVecs[8]->setInt(i, i * 8);
        columnVecs[9]->setInt(i, i * 9);
        columnVecs[10]->setInt(i, 1577836800 + i);      // 2020.01.01 00:00:00+i
        columnVecs[11]->setLong(i, Util::getEpochTime());
        columnVecs[12]->setLong(i, i * 12);
        columnVecs[13]->setLong(i, 1577836800000000000l + i); // 2020.01.01 00:00:00.000000000+i
        columnVecs[14]->setFloat(i, i * 14);
        columnVecs[15]->setDouble(i, i * 15);
        columnVecs[16]->setString(i, "sym"); //+ to_string(i));
        columnVecs[17]->setString(i, "abc"); //+ to_string(i));
        data[15] = i;
        columnVecs[18]->setBinary(i, 16, data);
        columnVecs[19]->setBinary(i, 16, data);
        columnVecs[20]->setBinary(i, 16, data);
    }
    return table;
}

TableSP createDemoTable2(long rows) {
    vector < string > colNames = { "cbool", "cchar", "cshort", "cint", "clong", "cdate", "cmonth", "ctime", "cminute", "csecond", "cdatetime", "ctimestamp", "cnanotime",
            "cnanotimestamp", "cfloat", "cdouble", "csymbol", "cstring", "cuuid", "cip", "cint128" };
    vector<DATA_TYPE> colTypes = { DT_BOOL, DT_CHAR, DT_SHORT, DT_INT, DT_LONG, DT_DATE, DT_MONTH, DT_TIME, DT_MINUTE, DT_SECOND, DT_DATETIME, DT_TIMESTAMP, DT_NANOTIME,
            DT_NANOTIMESTAMP, DT_FLOAT, DT_DOUBLE, DT_SYMBOL, DT_STRING, DT_UUID, DT_IP, DT_INT128 };
    int colNum = 21, rowNum = rows, indexCapacity = rows;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, indexCapacity);
    vector<VectorSP> columnVecs;
    for (int i = 0; i < colNum; i++)
        columnVecs.push_back(table->getColumn(i));

    char boolBuf[Util::BUF_SIZE];
    char charBuf[Util::BUF_SIZE];
    short shortBuf[Util::BUF_SIZE];
    int intBuf[Util::BUF_SIZE];
    long long longBuf[Util::BUF_SIZE];
    int dateBuf[Util::BUF_SIZE];
    int monthBuf[Util::BUF_SIZE];
    int timeBuf[Util::BUF_SIZE];
    int minuteBuf[Util::BUF_SIZE];
    int secondBuf[Util::BUF_SIZE];
    int datetimeBuf[Util::BUF_SIZE];
    long long timestampBuf[Util::BUF_SIZE];
    long long nanotimeBuf[Util::BUF_SIZE];
    long long nanotimeStampBuf[Util::BUF_SIZE];
    float floatBuf[Util::BUF_SIZE];
    double doubleBuf[Util::BUF_SIZE];
    unsigned char uuidBuf[Util::BUF_SIZE * 16];
    unsigned char ipBuf[Util::BUF_SIZE * 16];
    unsigned char int128Buf[Util::BUF_SIZE * 16];

    int start = 0;
    unsigned char data[16] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    while (start < rowNum) {
        size_t len = std::min(Util::BUF_SIZE, rowNum - start);
        char *pBool = columnVecs[0]->getBoolBuffer(start, len, boolBuf);
        char *pChar = columnVecs[1]->getCharBuffer(start, len, charBuf);
        short *pShort = columnVecs[2]->getShortBuffer(start, len, shortBuf);
        int *pInt = columnVecs[3]->getIntBuffer(start, len, intBuf);
        long long *pLong = columnVecs[4]->getLongBuffer(start, len, longBuf);
        int *pDate = columnVecs[5]->getIntBuffer(start, len, dateBuf);
        int *pMonth = columnVecs[6]->getIntBuffer(start, len, monthBuf);
        int *pTime = columnVecs[7]->getIntBuffer(start, len, timeBuf);
        int *pMinute = columnVecs[8]->getIntBuffer(start, len, minuteBuf);
        int *pSecond = columnVecs[9]->getIntBuffer(start, len, secondBuf);
        int *pDatetime = columnVecs[10]->getIntBuffer(start, len, datetimeBuf);
        long long *pTimestamp = columnVecs[11]->getLongBuffer(start, len, timestampBuf);
        long long *pNanotime = columnVecs[12]->getLongBuffer(start, len, nanotimeBuf);
        long long *pNanotimestamp = columnVecs[13]->getLongBuffer(start, len, nanotimeStampBuf);
        float *pFloat = columnVecs[14]->getFloatBuffer(start, len, floatBuf);
        double *pDouble = columnVecs[15]->getDoubleBuffer(start, len, doubleBuf);
        unsigned char *pUuid = columnVecs[18]->getBinaryBuffer(start, len, 16, uuidBuf);
        unsigned char *pIp = columnVecs[19]->getBinaryBuffer(start, len, 16, ipBuf);
        unsigned char *pInt128 = columnVecs[20]->getBinaryBuffer(start, len, 16, int128Buf);

        for (int i = 0; i < (int) len; ++i) {
            pBool[i] = i % 2;
            pChar[i] = i;
            pShort[i] = i * 2;
            pInt[i] = i * 3;
            pLong[i] = i * 4;
            pDate[i] = 18262;
            pMonth[i] = 24240;
            pTime[i] = i * 7;
            pMinute[i] = i * 8;
            pSecond[i] = i * 9;
            pDatetime[i] = 1577836800 + i;
            pTimestamp[i] = Util::getEpochTime() + i;
            pNanotime[i] = i * 12;
            pNanotimestamp[i] = 1577836800000000000l + i;
            pFloat[i] = i * 2;
            pDouble[i] = i * 2;

            columnVecs[16]->setString(i, "sym123"); //+ to_string(i));
            columnVecs[17]->setString(i, "abc123"); //+ to_string(i));
            data[15] = i;
            memcpy((void*) &pUuid[i * 16], (void*) data, 16);
            memcpy((void*) &pIp[i * 16], (void*) data, 16);
            memcpy((void*) &pInt128[i * 16], (void*) data, 16);

        }

        columnVecs[0]->setBool(start, len, pBool);
        columnVecs[1]->setChar(start, len, pChar);
        columnVecs[2]->setShort(start, len, pShort);
        columnVecs[3]->setInt(start, len, pInt);
        columnVecs[4]->setLong(start, len, pLong);
        columnVecs[5]->setInt(start, len, pDate);
        columnVecs[6]->setInt(start, len, pMonth);
        columnVecs[7]->setInt(start, len, pTime);
        columnVecs[8]->setInt(start, len, pMinute);
        columnVecs[9]->setInt(start, len, pSecond);
        columnVecs[10]->setInt(start, len, pDatetime);
        columnVecs[11]->setLong(start, len, pTimestamp);
        columnVecs[12]->setLong(start, len, pNanotime);
        columnVecs[13]->setLong(start, len, pNanotimestamp);
        columnVecs[14]->setFloat(start, len, pFloat);
        columnVecs[15]->setDouble(start, len, pDouble);
        columnVecs[18]->setBinary(start, len, 16, pUuid);
        columnVecs[19]->setBinary(start, len, 16, pIp);
        columnVecs[20]->setBinary(start, len, 16, pInt128);
        start += len;
    }
    return table;
}

int main(int argc, char *argv[]) {

    string host = "127.0.0.1";
    int port = 8848;
    long rows = 100000;
    long batches = 1000;

    DBConnection conn;
    try {
        bool ret = conn.connect(host, port, "admin", "123456");
        if (!ret) {
            cout << "Failed to connect to the server" << endl;
            return 0;
        }
    } catch (exception &ex) {
        cout << "Failed to  connect  with error: " << ex.what();
        return -1;
    }
    cout << "Please waiting..." << endl;

    long long startTime = Util::getEpochTime();
    for (unsigned int i = 0; i < batches; i++) {
        TableSP table = createDemoTable2(rows);
        vector<ConstantSP> args;
        args.push_back(table);
        conn.run("tableInsert{loadTable('dfs://demo', `pt)}", args);
    }
    long long endTime = Util::getEpochTime();
    printf("Insert %ld rows %ld batches, used %lld ms.\n", rows, batches, endTime - startTime);
    return 0;
}
