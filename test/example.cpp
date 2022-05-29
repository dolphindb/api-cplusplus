//
// Created by DolphinDB Inc. on 11/29/18.
//

#include "DolphinDB.h"
#include "Util.h"

#include <vector>
#include <string>
#include <iostream>

using namespace dolphindb;

int count = 0, countPass = 0;
// type T must support operator!=
template<typename T>
void TEST(const string &test, const T &ret, const T &expect) {
    if(ret != expect) {
        std::cout << "[FAIL] " << test << ": expect "<< expect << " --- actual " << ret << std::endl;
    }
    else {
        std::cout << "[PASS] " << test << std::endl;
        countPass++;
    }
    count++;
}

void testStringVector(DBConnection &conn);
void testDoubleVector(DBConnection &conn);
void testIntSet(DBConnection &conn);
void testIntMatrix(DBConnection &conn);
void testDictionary(DBConnection &conn);
void testTable(DBConnection &conn);
void testVoid(DBConnection &conn);
void testIntNull(DBConnection &conn);
void testCallFunction(DBConnection &conn);
void testUpload(DBConnection &conn);
void testMemoryTable(DBConnection &conn);
void testDFSTable(DBConnection &conn);
void testCreateTable(DBConnection &conn);
void testUseTable(DBConnection &conn);
void testTimeTypes(DBConnection &conn);

int main(int argc, char *argv[])
{
    DBConnection conn;
    DBConnection::initialize();
    bool success = conn.connect("localhost", 8848, "admin", "123456");  //http://115.239.209.223:8958
    testStringVector(conn);
    testDoubleVector(conn);
    testIntSet(conn);
    testIntMatrix(conn);
    testDictionary(conn);
    testTable(conn);
    testVoid(conn);
    testIntNull(conn);
    testCallFunction(conn);
    testUpload(conn);
    testMemoryTable(conn);
    //testDFSTable(conn);           // to run this test, the DolphinDB must run as a cluster
    testCreateTable(conn);
    testUseTable(conn);
    testTimeTypes(conn);
    std::cout << "[PASS " << countPass << "/" << count << "]" << std::endl;
    return 0;
}

void testStringVector(DBConnection &conn) {
    ConstantSP vector = conn.run("rand(`IBM`MSFT`GOOG`BIDU, 10)");

    int size = vector->size();
    for(int i = 0; i < size; ++i) {
        std::cout << vector->getString(i) << " ";
    }
    std::cout << std::endl;
    TEST("testStringVector", size, 10);
}

void testDoubleVector(DBConnection &conn) {
    ConstantSP vector = conn.run("rand(10.0, 10)");

    int size = vector->size();
    for(int i = 0; i < size; ++i) {
        std::cout << vector->getDouble(i) << " ";
    }
    std::cout << std::endl;
    TEST("testDoubleVector", size, 10);
}

void testIntSet(DBConnection &conn) {
    ConstantSP set = conn.run("set(1+3*1..10)");
    std::cout << set->getString() << std::endl;
    TEST("testIntSet", set->size(), 10);
}

void testIntMatrix(DBConnection &conn) {
    ConstantSP matrix = conn.run("1..6$3:2");
    std::cout << matrix->getString() << std::endl;
    TEST("testIntMatrix", matrix->isMatrix(), true);
}

void testDictionary(DBConnection &conn) {
    ConstantSP dict = conn.run("dict(1 2 3,`IBM`MSFT`GOOG)");
    std::cout << dict->getString() << std::endl;
    TEST("testDictionary", ((Dictionary *)dict.get())->itemCount(), 3);
}

void testTable(DBConnection &conn) {
    string script;
    script += "n=2000\n";
    script += "syms=`IBM`C`MS`MSFT`JPM`ORCL\n";
    script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms, n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price)\n";
    script += "select qty,price,sym from mytrades";
    ConstantSP table = conn.run(script);
    std::cout << table->getString() << std::endl;
    TEST("testTable", table->rows(), 2000);
}

void testVoid(DBConnection &conn) {
    ConstantSP null = conn.run("NULL");
    TEST("testVoid", null->getType(), DT_VOID);
}

void testIntNull(DBConnection &conn) {
    ConstantSP null = conn.run("int()");
    null->setNull();
    TEST("testIntNull", null->getInt(), INT32_MIN);
}

void testCallFunction(DBConnection &conn) {
    VectorSP vec = Util::createVector(DT_DOUBLE, 2);
    double data[] = {3.5, 4.5};
    vec->setDouble(0, 1.5);
    vec->setDouble(1, 2.5);
    vec->appendDouble(data, 2);

    std::vector<ConstantSP> args{vec};
    ConstantSP result = conn.run("sum", args);
    TEST("testCallFunction", result->getDouble(), 12.0);
}

void testUpload(DBConnection &conn) {
    ConstantSP vec = Util::createVector(DT_DOUBLE, 0);
    double data[] = {1.5, 2.5, 3.5, 4.5};
    ((Vector *)vec.get())->appendDouble(data, 4);

    std::vector<std::string> vars{"a"};
    std::vector<ConstantSP> args{vec};
    conn.upload(vars, args);
    ConstantSP result = conn.run("accumulate(+,a)");
    std::cout << result->getString() << std::endl;
    TEST("testUpload", result->getDouble(3), 12.0);
}

void testMemoryTable(DBConnection &conn) {
    // assume the specific memory table is already exists in the dolphindb
    try {
        std::string pre = "t = table(1000:0, `cstring`cint`ctimestamp`cdouble,[STRING,INT,TIMESTAMP,DOUBLE])\n"
                          "share t as sharedTable";
        conn.run(pre);

        // insert into
        std::string s = "'test1'";
        int i = 1;
        long l = 10;
        double d = 11.0;
        std::string script;
        script += "insert into sharedTable values(";
        script += s + "," + std::to_string(i) + "," + std::to_string(l) + "," + std::to_string(d) +")";
        conn.run(script);                                                     // insert 1 row, now 1 row

        // tableInsert
        ConstantSP col0 = Util::createVector(DT_STRING, 1);
        col0->setString(0, "test2");
        ConstantSP col1 = Util::createVector(DT_INT, 1);
        col1->setInt(0, 2);
        ConstantSP col2 = Util::createVector(DT_TIMESTAMP, 1);
        col2->setLong(0, 20);
        ConstantSP col3 = Util::createVector(DT_DOUBLE, 1);
        col3->setDouble(0, 22.0);

        vector<ConstantSP> cols{col0, col1, col2, col3};
        ConstantSP result1 = conn.run("tableInsert{sharedTable}", cols);      // insert 1 row, now 2 rows

        // append!
        ConstantSP myTable = conn.run("select * from sharedTable");           // get 2 rows table
        std::vector<ConstantSP> arg{myTable};
        ConstantSP result2 = conn.run("append!{sharedTable}", arg);           // insert 2 rows table,  now 4 rows
        TEST("testMemoryTable", 2 * result1->getInt(), myTable->rows());
    } catch (IOException ex) {
        std::cout << "exception: " << ex.what() << std::endl;
        TEST("testMemoryTable", string("IOException"), string());
    }
}

void testDFSTable(DBConnection &conn) {
    try {
        ConstantSP myTable = conn.run("select * from sharedTable");
        vector<ConstantSP> args{myTable};
        conn.run("append!{loadTable('dfs://testDatabase','tb1')}", args);
        TEST("testDFSTable", 1, 1);
    } catch (IOException ex) {
        std::cout << "exception: " << ex.what() << std::endl;
        TEST("testDFSTable", string("IOException"), string());
    }
}

void testCreateTable(DBConnection &conn) {
    std::vector<char> cbool{1,0,1};
    std::vector<int> cint{29,37,INT32_MIN};
    std::vector<double> cdouble{4.9,1.2,7.7};

    std::vector<std::string> colNames = {"cbool", "cint", "cdouble"};
    std::vector<ConstantSP> cols;
    cols.push_back(Util::createVector(DT_BOOL, 3));
    cols[0]->setBool(0, 3, cbool.data());
    cols.push_back(Util::createVector(DT_INT, 3));
    cols[1]->setInt(0, 3, cint.data());
    cols.push_back(Util::createVector(DT_DOUBLE, 3));
    cols[2]->setDouble(0, 3, cdouble.data());
    ConstantSP table = Util::createTable(colNames, cols);
    TEST("testCreateTable", table->getColumn(1)->get(2)->isNull(), true);
}

void testUseTable(DBConnection &conn) {
    ConstantSP table = conn.run("select * from sharedTable");
    vector<ConstantSP> cols;
    for(int i = 0; i < table->columns(); ++i) {
        cols.emplace_back(table->getColumn(i));
    }
    for(int row = 0; row < table->rows(); ++row) {
        std::cout << "row " << row << ":";
        for(int col = 0; col < cols.size(); ++col) {
            std::cout << " " << cols[col]->get(row)->getString();
        }
        std::cout << std::endl;
    }
    TEST("testUseTable", table->isTable(), true);
}

void testTimeTypes(DBConnection &conn) {
    ConstantSP date = Util::createDate(1970, 1, 2);                             //1970.01.02
    TEST("testDate", date->getInt(), 1);

    ConstantSP month = Util::createMonth(2018, 1);                              //2018.01M
    TEST("testMonth", month->getInt(), 2018*12);

    ConstantSP time = Util::createTime(1, 1, 1, 1);                             //01:01:01:001
    TEST("testTime", time->getInt(), 3600000+60000+1000+1);

    ConstantSP minute = Util::createMinute(2, 30);                              //02:30m
    TEST("testMinute", minute->getInt(), 2*60+30);

    ConstantSP second = Util::createSecond(5,6,7);                              //05:06:07
    TEST("testSecond", second->getInt(), 5*3600+6*60+7);

    ConstantSP datetime = Util::createDateTime(1970, 1, 1, 1, 1, 1);            //1970.01.01T01:01:01
    TEST("testDatetime", datetime->getInt(),3600+60+1);

    ConstantSP timestamp = Util::createTimestamp(1970, 1, 1, 1, 1, 1, 123);     //1970.01.01T01:01:01.123
    TEST("testTimestamp", timestamp->getLong(), (long long)3600000+60000+1000+123);

    ConstantSP nanotime = Util::createNanoTime(0, 0, 1, 123);                   //00:00:01.000000123
    TEST("testNanotime", nanotime->getLong(), (long long)1000000123);

    ConstantSP nanotimestamp = Util::createNanoTimestamp(1970,1,1,0,0,0,567);   //1970.01.01T00:00:00.000000567
    TEST("testNanotimestamp", nanotimestamp->getLong(), (long long)567);
}

