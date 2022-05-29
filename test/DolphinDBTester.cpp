#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include <vector>
#include <limits.h>
#include <assert.h>

using namespace dolphindb;
using namespace std;
static string hostName = "localhost";
static int port = 8848;
static string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static int pass, fail;

DBConnection conn;

string genRandString(int maxSize) {
    string result;
    int size = rand() % maxSize;
    for (int i = 0; i < size; i++) {
        int r = rand() % alphas.size();
        result += alphas[r];
    }
    return result;
}

template<typename T>
void ASSERTION(const string test, const T &ret, const T &expect) {
    if (ret != expect) {
        std::cout << "ASSERT FAIL--" << test << "-- expect return --" << (T) expect << ", real return--" << (T) ret << std::endl;
        fail++;
    } else
        pass++;

}

template<typename T>
void ASSERTIONARR(const string test, const T ret[], const T expect[], int len) {
    bool equal = true;
    for (int i = 0; i < len; i++) {
        if (ret[i] != expect[i])
            equal = false;
    }

    if (!equal) {
        std::cout << "ASSERT FAIL--" << test << "-- expect return --";
        for (int i = 0; i < len; i++)
            std::cout << expect[i] << ",";
        cout << ", real return--";
        for (int i = 0; i < len; i++)
            std::cout << ret[i] << ",";
        cout << std::endl;
        fail++;
    } else
        pass++;

}

void printTestResults() {
    cout << "total:" << std::to_string(pass + fail) << endl;
    cout << "fail:" << std::to_string(fail) << endl;
    cout << "pass:" << std::to_string(pass) << endl;
}

void testStringVector(int vecSize) {
    vector < string > values;
    for (int i = 0; i < vecSize; i++)
        values.push_back(genRandString(30));
    string script;
    for (int i = 0; i < vecSize; i++)
        script += "`" + values[i];
    ConstantSP result = conn.run(script);
    for (int i = 0; i < vecSize; i++) {
        ASSERTION("testStringVector", result->getString(i), values[i]);
    }
}

void testIntVector(int vecSize) {
    vector<int> values;
    for (int i = 0; i < vecSize; i++)
        values.push_back(rand() % INT_MAX);
    string script;
    for (int i = 0; i < vecSize; i++)
        script += " " + std::to_string(values[i]);
    ConstantSP result = conn.run(script);
    for (int i = 0; i < vecSize; i++)
        ASSERTION("testIntVector", result->getInt(i), values[i]);
}

void testDoubleVector(int vecSize) {
    vector<double> values;
    for (int i = 0; i < vecSize; i++)
        values.push_back((double) (rand()));
    string script;
    for (int i = 0; i < vecSize; i++)
        script += " " + std::to_string(values[i]);
    ConstantSP result = conn.run(script);
    for (int i = 0; i < vecSize; i++)
        ASSERTION("testDoubleVector", result->getDouble(i), values[i]);
}

void testDateVector() {
    string beginDate = "2010.08.20";
    vector<int> testValues = { 1, 10, 100, 1000, 10000, 100000 };
    vector < string > expectResults = { "2010.08.21", "2010.08.30", "2010.11.28", "2013.05.16", "2038.01.05", "2284.06.04" };
    string script;
    for (unsigned int i = 0; i < testValues.size(); i++) {
        script += " " + std::to_string(testValues[i]);
    }
    script = beginDate + " + " + script;
    ConstantSP result = conn.run(script);
    for (unsigned int i = 0; i < testValues.size(); i++) {
        ASSERTION("testDateVector", result->getString(i), expectResults[i]);
    }
}

void testDatetimeVector() {
    string beginDateTime = "2012.10.01 15:00:04";
    vector<int> testValues = { 1, 100, 1000, 10000, 100000, 1000000, 10000000 };
    vector < string > expectResults = { "2012.10.01T15:00:05", "2012.10.01T15:01:44", "2012.10.01T15:16:44", "2012.10.01T17:46:44", "2012.10.02T18:46:44", "2012.10.13T04:46:44",
            "2013.01.25T08:46:44" };
    string script;
    for (unsigned int i = 0; i < testValues.size(); i++) {
        script += " " + std::to_string(testValues[i]);
    }
    script = beginDateTime + " + " + script;
    ConstantSP result = conn.run(script);
    for (unsigned int i = 0; i < testValues.size(); i++) {
        ASSERTION("testDatetimeVector", result->getString(i), expectResults[i]);
    }
}

void testTimeStampVector() {
    string beginTimeStamp = "2009.10.12T00:00:00.000";
    vector<long> testValues = { 1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000, 100000000000, 1000000000000 };
    vector < string > expectResults = { "2009.10.12T00:00:00.001", "2009.10.12T00:00:00.010", "2009.10.12T00:00:00.100", "2009.10.12T00:00:01.000", "2009.10.12T00:00:10.000",
            "2009.10.12T00:01:40.000", "2009.10.12T00:16:40.000", "2009.10.12T02:46:40.000", "2009.10.13T03:46:40.000", "2009.10.23T13:46:40.000", "2010.02.04T17:46:40.000",
            "2012.12.12T09:46:40.000", "2041.06.20T01:46:40.000" };
    string script;
    for (unsigned int i = 0; i < testValues.size(); i++) {
        script += " " + std::to_string(testValues[i]);
    }
    script = beginTimeStamp + " + " + script;
    ConstantSP result = conn.run(script);
    for (unsigned int i = 0; i < testValues.size(); i++) {
        ASSERTION("testTimeStampVector", result->getString(i), expectResults[i]);
    }
}

void testFunctionDef() {
    string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
    ConstantSP result = conn.run(script);
    ASSERTION("testFunctionDef", result->getString(), string("300"));
}

void testMatrix() {
    vector < string > expectResults = { "{1,2}", "{3,4}", "{5,6}" };
    string script = "1..6$2:3";
    ConstantSP result = conn.run(script);
    for (unsigned int i = 0; i < expectResults.size(); i++) {
        ASSERTION("testMatrix", result->getString(i), expectResults[i]);
    }
}

void testTable() {
    string script;
    script += "n=20000\n";
    script += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n";
    script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price, 1..n as number);\n";
    script += "select min(number) as minNum, max(number) as maxNum from mytrades";

    ConstantSP table = conn.run(script);
    ASSERTION("testTable", table->getColumn(0)->getString(0), string("1"));
    ASSERTION("testTable", table->getColumn(1)->getString(0), string("20000"));
}

void testDictionary() {
    string script;
    script += "dict(1 2 3,`IBM`MSFT`GOOG)";
    DictionarySP dict = conn.run(script);

    ASSERTION("testDictionary", dict->get(Util::createInt(1))->getString(), string("IBM"));
    ASSERTION("testDictionary", dict->get(Util::createInt(2))->getString(), string("MSFT"));
    ASSERTION("testDictionary", dict->get(Util::createInt(3))->getString(), string("GOOG"));
}

void testSet() {
    string script;
    script += "x=set(4 5 5 2 3 11 11 11 6 6  6 6  6);x;";
    ConstantSP set = conn.run(script);
    ASSERTION("testSet", set->size(), 6);
}

void testMemoryTable() {
    string script;
    //simulation to generate data to be saved to the memory table
    VectorSP names = Util::createVector(DT_STRING, 5, 100);
    VectorSP dates = Util::createVector(DT_DATE, 5, 100);
    VectorSP prices = Util::createVector(DT_DOUBLE, 5, 100);
    for (int i = 0; i < 5; i++) {
        names->set(i, Util::createString("name_" + std::to_string(i)));
        dates->set(i, Util::createDate(2010, 1, i + 1));
        prices->set(i, Util::createDouble(i * i));
    }
    vector < string > allnames = { "names", "dates", "prices" };
    vector<ConstantSP> allcols = { names, dates, prices };
    conn.upload(allnames, allcols); //upload data to server
    script += "insert into tglobal values(names,dates,prices);";
    script += "select * from tglobal;";
    TableSP table = conn.run(script);
    cout << table->getString() << endl;
}

TableSP createDemoTable() {
    vector < string > colNames = { "name", "date", "price" };
    vector<DATA_TYPE> colTypes = { DT_STRING, DT_DATE, DT_DOUBLE };
    int colNum = 3, rowNum = 3;
    ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
    vector<VectorSP> columnVecs;
    for (int i = 0; i < colNum; i++)
        columnVecs.push_back(table->getColumn(i));

    for (int i = 0; i < rowNum; i++) {
        columnVecs[0]->set(i, Util::createString("name_" + std::to_string(i)));
        columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
        columnVecs[2]->set(i, Util::createDouble(i * i));
    }
    return table;
}

void testDiskTable() {
    TableSP table = createDemoTable();
    conn.upload("mt", table);
    string script;
    script += "db=database(\"/home/psui/demoTable1\");";
    script += "tDiskGlobal.append!(mt);";
    script += "saveTable(db,tDiskGlobal,`dt);";
    script += "select * from tDiskGlobal;";
    TableSP result = conn.run(script);
    cout << result->getString() << endl;
}

void testDFSTable() {
    string script;
    TableSP table = createDemoTable();
    conn.upload("mt", table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
    script += "tableName = `demoTable;";
    script += "database(dbPath).loadTable(tableName).append!(mt);";
    script += "tradTable= database(dbPath).loadTable(tableName);";
    script += "select * from tradTable;";
    TableSP result = conn.run(script);
    cout << result->getString() << endl;
}

void ASSERTIONHASH(const string test, const int ret[], const int expect[], int len) {
    bool equal = true;
    for (int i = 0; i < len; i++) {
        if (ret[i] != expect[i])
            equal = false;
    }

    if (!equal) {
        std::cout << "ASSERT FAIL--" << test << "-- expect return --";
        for (int i = 0; i < len; i++)
            std::cout << expect[i] << ",";
        cout << ", real return--";
        for (int i = 0; i < len; i++)
            std::cout << ret[i] << ",";
        cout << std::endl;
        fail++;
    } else
        pass++;

}
void testCharVectorHash() {
    vector<char> testValues { 127, -127, 12, 0, -12, -128 };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 10, 12, 12, 0, 10, -1 }, { 41, 18, 12, 0, 4, -1 }, { 56, 24, 12, 0, 68, -1 }, { 30, 5, 12, 0, 23, -1 }, { 127, 129, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createChar(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
        }
        ASSERTIONHASH("testCharVectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_CHAR, 0);
    v->appendChar(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testCharVectorHash", hv, expected[j], 6);
    }
}

void testShortVectorHash() {
    vector<short> testValues { 32767, -32767, 12, 0, -12, -32768 };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 7, 2, 12, 0, 10, -1 }, { 1, 15, 12, 0, 4, -1 }, { 36, 44, 12, 0, 68, -1 }, { 78, 54, 12, 0, 23, -1 }, { 4088, 265, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createShort(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
        }
        ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_SHORT, 0);
    v->appendShort(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
    }
}
void testIntVectorHash() {
    vector<int> testValues { INT_MAX, INT_MAX * (-1), 12, 0, -12, INT_MIN };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 10, 12, 12, 0, 10, -1 }, { 7, 9, 12, 0, 4, -1 }, { 39, 41, 12, 0, 68, -1 }, { 65, 67, 12, 0, 23, -1 }, { 127, 129, 12, 0, 244, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createInt(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
        }
        ASSERTIONHASH("testIntVectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_INT, 0);
    v->appendInt(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testIntVectorHash", hv, expected[j], 6);
    }
}

void testLongVectorHash() {
    vector<long long> testValues { LLONG_MAX, (-1) * LLONG_MAX, 12, 0, -12, LLONG_MIN };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 7, 9, 12, 0, 4, -1 }, { 41, 0, 12, 0, 29, -1 }, { 4, 6, 12, 0, 69, -1 }, { 78, 80, 12, 0, 49, -1 }, { 4088, 4090, 12, 0, 4069, -1 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createLong(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
        }
        ASSERTIONHASH("testLongVectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_LONG, 0);
    v->appendLong(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testLongVectorHash", hv, expected[j], 6);
    }
}

void testStringVectorHash() {
    vector < string > testValues { "9223372036854775807", "helloworldabcdefghijklmnopqrstuvwxyz", "智臾科技", "hello,智臾科技", "123abc您好！", "" };
    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int expected[5][6] = { { 8, 1, 11, 8, 10, 0 }, { 37, 20, 14, 23, 41, 0 }, { 31, 0, 41, 63, 40, 0 }, { 24, 89, 51, 54, 42, 0 }, { 739, 3737, 814, 3963, 3488, 0 } };
    int hv[6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (unsigned int i = 0; i < testValues.size(); i++) {
            ConstantSP val = Util::createString(testValues[i]);
            hv[i] = val->getHash(buckets[j]);
        }
        ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_STRING, 0);
    v->appendString(testValues.data(), testValues.size());
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
    }
}
void testUUIDvectorHash() {
    string script;
    script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
        }
        ASSERTIONHASH("testUUIDvectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_UUID, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testUUIDvectorHash", hv, expected[j], 6);
    }

}
void testIpAddrvectorHash() {
    string script;
    script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_IP, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
        }
        ASSERTIONHASH("testIpAddrvectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_IP, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_IP, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testIpAddrvectorHash", hv, expected[j], 6);
    }

}
void testInt128vectorHash() {
    string script;
    script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5] = { 13, 43, 71, 97, 4097 };
    int hv[6] = { 0 };
    int expected[5][6] = { 0 };

    for (unsigned int j = 0; j < 5; j++) {
        for (int i = 0; i < t->size(); i++) {
            ConstantSP val = Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i));
            hv[i] = val->getHash(buckets[j]);
            expected[j][i] = t->getColumn(j + 1)->getInt(i);
        }
        ASSERTIONHASH("testInt128vectorHash", hv, expected[j], 6);
    }
    VectorSP v = Util::createVector(DT_INT128, 0);
    for (int i = 0; i < t->size(); i++)
        v->append(Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i)));
    for (unsigned int j = 0; j < 5; j++) {
        v->getHash(0, 6, buckets[j], hv);
        ASSERTIONHASH("testInt128vectorHash", hv, expected[j], 6);
    }

}
char* random_uuid(char buf[37]) {
    const char *c = "89ab";
    char *p = buf;
    int n;
    for (n = 0; n < 16; ++n) {
        int b = rand() % 255;
        switch (n) {
        case 6:
            sprintf(p, "4%x", b % 15);
            break;
        case 8:
            sprintf(p, "%c%x", c[rand() % strlen(c)], b % 15);
            break;
        default:
            sprintf(p, "%02x", b);
            break;
        }
        p += 2;
        switch (n) {
        case 3:
        case 5:
        case 7:
        case 9:
            *p++ = '-';
            break;
        }
    }
    *p = 0;
    return buf;
}
void testScalar() {
    int i;

    int intVal[2] = { 0x7fffffff, -0x7fffffff };
    long long longVal[2] = { 0x7fffffffffffffff, -0x7fffffffffffffff }; //9223372036854775807
    for (i = 0; i < 2; i++) {
        ConstantSP spInt = Util::createInt(intVal[i]);
        ASSERTION("testConstantInt1", spInt->getInt(), intVal[i]);
        spInt = Util::parseConstant(DT_INT, std::to_string(intVal[i]));
        ASSERTION("testConstantInt2", spInt->getInt(), intVal[i]);
        assert(spInt->isScalar() && spInt->getForm() == DF_SCALAR && spInt->getType() == DT_INT);

        ConstantSP spLong = Util::createLong(longVal[i]);
        ASSERTION("testConstantLong1", spLong->getLong(), longVal[i]);
        spLong = Util::parseConstant(DT_LONG, std::to_string(longVal[i]));
        ASSERTION("testConstantLong2", spLong->getLong(), longVal[i]);
        assert(spLong->isScalar() && spLong->getForm() == DF_SCALAR && spLong->getType() == DT_LONG);

        ConstantSP spFloat = Util::createFloat(intVal[i]+0.12345);
        ASSERTION("testConstantFloat1", spFloat->getFloat(), (float)(intVal[i]+0.12345));
        spFloat = Util::parseConstant(DT_FLOAT, std::to_string(intVal[i]+0.12345));
        ASSERTION("testConstantFloat2", spFloat->getFloat(), (float)(intVal[i]+0.12345));
        assert(spFloat->isScalar() && spFloat->getForm() == DF_SCALAR && spFloat->getType() == DT_FLOAT);

        ConstantSP spDouble = Util::createDouble(longVal[i]+0.12345);
        ASSERTION("testConstantDouble1", spDouble->getDouble(), (double)(longVal[i]+0.12345));
        spDouble = Util::parseConstant(DT_DOUBLE, std::to_string(longVal[i]+0.12345));
        ASSERTION("testConstantDouble2", spDouble->getDouble(), (double)(longVal[i]+0.12345));
        assert(spDouble->isScalar() && spDouble->getForm() == DF_SCALAR && spDouble->getType() == DT_DOUBLE);

    }
    ConstantSP sp=Util::parseConstant(DT_FLOAT, "a.1");
    cout<<sp->getFloat()<<endl;

    ConstantSP spIP = Util::createConstant(DT_IP);
    unsigned char ip[16] = { 0 };
    for (i = 0; i < 16; i++)
        ip[i] = i;
    spIP->setBinary(0, 16, ip);
    ASSERTION("testConstantIPaddr1", spIP->getString(), string("f0e:d0c:b0a:908:706:504:302:100"));
    assert(spIP->isScalar() && spIP->getForm() == DF_SCALAR && spIP->getType() == DT_IP);

    spIP = Util::parseConstant(DT_IP, "f0e:d0c:b0a:908:706:504:302:100"); //"192.168.2.1");
    assert(spIP->isScalar() && spIP->getForm() == DF_SCALAR && spIP->getType() == DT_IP);
    unsigned char *bi = (unsigned char*) spIP->getBinary();
    ASSERTIONARR("testConstantIPaddr2", bi, ip, 16);
    spIP = Util::parseConstant(DT_IP, "192.168.2.1");
    assert(spIP->isScalar() && spIP->getForm() == DF_SCALAR && spIP->getType() == DT_IP);
    memset(ip, 0, 16);
    ip[0] = 1, ip[1] = 2, ip[2] = 168, ip[3] = 192;
    bi = (unsigned char*) spIP->getBinary();
    ASSERTIONARR("testConstantIPaddr3", bi, ip, 16);
    spIP = Util::parseConstant(DT_IP, "90:b0a:908:706:504:302:100");
    assert(spIP.isNull());
    spIP = Util::parseConstant(DT_IP, "0:hh:b0a:908:706:504:302:100");
    assert(spIP.isNull());

    //spIP=Util::parseConstant(DT_IP, "::b0a:908:706:504:302:100");
    //assert(spIP.isNull());

    spIP = Util::parseConstant(DT_IP, "h.168.2.1");
    assert(spIP.isNull());
    spIP = Util::parseConstant(DT_IP, "h.192.168.2.1");
    assert(spIP.isNull());

    ConstantSP spUuid = Util::createConstant(DT_UUID);
    for (i = 0; i < 16; i++)
        ip[i] = i;
    spUuid->setBinary(0, 16, ip);
    assert(spUuid->isScalar() && spUuid->getForm() == DF_SCALAR && spUuid->getType() == DT_UUID);
    bi = (unsigned char*) spUuid->getBinary();
    ASSERTIONARR("testConstantUUDI1", bi, ip, 16);
    char guid[37];
    for (i = 0; i < 100; i++) {
        random_uuid(guid);
        spUuid = Util::parseConstant(DT_UUID, string(guid));
        assert(spUuid->isScalar() && spUuid->getForm() == DF_SCALAR && spUuid->getType() == DT_UUID);
        ASSERTION("testConstantUUDI2", spUuid->getString(), string(guid));
    }
    spUuid = Util::parseConstant(DT_UUID, "5b2de9b4-3471-4e66-a019-d8e3f1222a58");
    ASSERTION("testUUID2", spUuid->getString(), string("5b2de9b4-3471-4e66-a019-d8e3f1222a58"));

    try {
        spUuid = Util::parseConstant(DT_UUID, "hi2de9b4-3471-4e66-a019-d8e3f1222a58");
        fail++;
        cout << "Invalid uuid string ,but passed :" << spUuid->getString() << endl;

        spUuid = Util::parseConstant(DT_UUID, "5a2de9b4-071-4e66-a019-d8e3f1222a58");
        fail++;
        cout << "Invalid UUID string,but passed." << spUuid->getString() << endl;
    } catch (exception &ex) {
        cout << "Failed to  parseConstrant: " << ex.what() << endl;
    }

    ConstantSP spInt128 = Util::createConstant(DT_INT128);
    for (i = 0; i < 16; i++)
        ip[i] = i;
    spInt128->setBinary(0, 16, ip);
    assert(spInt128->isScalar() && spInt128->getForm() == DF_SCALAR && spInt128->getType() == DT_INT128);
    bi = (unsigned char*) spInt128->getBinary();
    ASSERTIONARR("testConstantInt1281", bi, ip, 16);

    spInt128 = Util::parseConstant(DT_INT128, "34f0302cae07db8201d895e3acc0c703");
    ASSERTION("testInt1282", spInt128->getString(), string("34f0302cae07db8201d895e3acc0c703"));

    try {
        spInt128 = Util::parseConstant(DT_INT128, "hi2de9b4-3471-4e66-a019-d8e3f1222a58");
        if (!spInt128.isNull()) {
            fail++;
            cout << "Invalid int128 string ,but passed :" << spInt128->getString() << endl;
        }

        spUuid = Util::parseConstant(DT_INT128, "5a2de9b4-071-4e66-a019-d8e3f1222a58");
        if (!spInt128.isNull()) {
            fail++;
            cout << "Invalid int128 string ,but passed :" << spInt128->getString() << endl;
        }
    } catch (exception &ex) {
        cout << "Failed to  parseConstrant: " << ex.what() << endl;
    }
}
int main() {

    testScalar();

    DBConnection::initialize();
    bool ret = conn.connect(hostName, port);
    if (!ret) {
        cout << "Failed to connect to the server" << endl;
        return 0;
    }
    int testVectorSize = 20;

    testStringVector(testVectorSize);
    testIntVector(testVectorSize);
    testDoubleVector(testVectorSize);
    testDateVector();
    testDatetimeVector();
    testTimeStampVector();
    testFunctionDef();
    testMatrix();
    testTable();
    testDictionary();
    testSet();
    //testMemoryTable();
    //testDiskTable();
    //testDFSTable();

    testCharVectorHash();
    testShortVectorHash();
    testIntVectorHash();
    testLongVectorHash();
    testStringVectorHash();
    testUUIDvectorHash();
    testIpAddrvectorHash();
    testInt128vectorHash();

    printTestResults();
    return 0;
}
