#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include <vector>
#include <limits.h>
using namespace dolphindb;
using namespace std;
static string hostName = "localhost";
static int port = 8848;
static string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
static int pass,fail;

DBConnection conn;

string genRandString(int maxSize){
    string result;
    int size = rand()%maxSize;
    for(int i = 0 ;i < size ;i ++){
        int r = rand() % alphas.size(); 
        result += alphas[r];
    }
    return result;
}

template<typename T>
void ASSERTION(const string test, const T& ret, const T& expect) {

    if(ret != expect){
        std::cout<<"ASSERT FAIL--" << test<< "-- expect return --"<< (T)expect <<", real return--"<<(T)ret<< std::endl;
        fail++;
    }
    else    
        pass++;

}

void printTestResults(){
    cout<<"total:"<<std::to_string(pass + fail)<<endl;
    cout<<"fail:"<<std::to_string(fail)<<endl;
    cout<<"pass:"<<std::to_string(pass)<<endl;
}


void testStringVector(int vecSize){
    vector<string> values;
    for(int i = 0 ;i < vecSize; i++)
        values.push_back(genRandString(30));
    string script; 
    for(int i = 0 ;i < vecSize; i++)
        script += "`" + values[i];
    ConstantSP result = conn.run(script);
    for(int i = 0; i < vecSize; i++){
        ASSERTION("testStringVector",result->getString(i),values[i]);
    }
}

void testIntVector(int vecSize){
    vector<int> values;
    for(int i = 0 ;i < vecSize ; i++)
        values.push_back(rand()%INT_MAX);
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + std::to_string(values[i]);
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testIntVector",result->getInt(i),values[i]);
}

void testDoubleVector(int vecSize){
    vector<double> values;
    for(int i = 0 ;i < vecSize; i++)
        values.push_back((double)(rand()));
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + std::to_string(values[i]);
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testDoubleVector",result->getDouble(i),values[i]);
}


void testDateVector(){
    string beginDate = "2010.08.20";
    vector<int> testValues = {1,10,100,1000,10000,100000};
    vector<string> expectResults = {"2010.08.21","2010.08.30","2010.11.28","2013.05.16","2038.01.05","2284.06.04"};
    string script;
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        script += " "+ std::to_string(testValues[i]);
    }
    script = beginDate + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        ASSERTION("testDateVector",result->getString(i),expectResults[i]);
    }
}



void testDatetimeVector(){
    string beginDateTime = "2012.10.01 15:00:04";
    vector<int> testValues = {1,100,1000,10000,100000,1000000,10000000};
    vector<string> expectResults = {"2012.10.01T15:00:05","2012.10.01T15:01:44","2012.10.01T15:16:44","2012.10.01T17:46:44","2012.10.02T18:46:44","2012.10.13T04:46:44","2013.01.25T08:46:44"};
    string script;
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        script += " "+ std::to_string(testValues[i]);
    }
    script = beginDateTime + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < testValues.size(); i++){
        ASSERTION("testDatetimeVector",result->getString(i),expectResults[i]);
    }
}


void testTimeStampVector(){
    string beginTimeStamp = "2009.10.12T00:00:00.000";
    vector<long> testValues = {1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000};
    vector<string> expectResults = {"2009.10.12T00:00:00.001","2009.10.12T00:00:00.010","2009.10.12T00:00:00.100",
            "2009.10.12T00:00:01.000","2009.10.12T00:00:10.000","2009.10.12T00:01:40.000","2009.10.12T00:16:40.000",
            "2009.10.12T02:46:40.000","2009.10.13T03:46:40.000","2009.10.23T13:46:40.000","2010.02.04T17:46:40.000",
            "2012.12.12T09:46:40.000","2041.06.20T01:46:40.000"};
    string script;
    for(unsigned int i = 0; i < testValues.size(); i++){
       script += " " + std::to_string(testValues[i]);
    }
    script = beginTimeStamp + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0; i < testValues.size(); i++){
        ASSERTION("testTimeStampVector",result->getString(i),expectResults[i]);
    }
}

void testFunctionDef(){
    string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
    ConstantSP result = conn.run(script);
    ASSERTION("testFunctionDef",result->getString(),string("300"));
}

void testMarics(){
    vector<string> expectResults = {"{1,2}","{3,4}","{5,6}"};
    string script = "1..6$2:3";
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < expectResults.size(); i++){
        ASSERTION("testMarics",result->getString(i),expectResults[i]);
    }
}



void testTable(){
    string script;
    script += "n=20000\n";
    script += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n";
    script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price, 1..n as number);\n";
    script += "select min(number) as minNum, max(number) as maxNum from mytrades";

    ConstantSP table = conn.run(script);
    ASSERTION("testTable",table->getColumn(0)->getString(0),string("1"));
    ASSERTION("testTable",table->getColumn(1)->getString(0),string("20000"));
}


void testDictionary(){
    string script;
    script += "dict(1 2 3,`IBM`MSFT`GOOG)";
    DictionarySP dict = conn.run(script);

    ASSERTION("testDictionary",dict->get(Util::createInt(1))->getString(),string("IBM"));
    ASSERTION("testDictionary",dict->get(Util::createInt(2))->getString(),string("MSFT"));
    ASSERTION("testDictionary",dict->get(Util::createInt(3))->getString(),string("GOOG"));
}

void testSet(){
    string script;
    script += "x=set(4 5 5 2 3 11 11 11 6 6  6 6  6);x;";
    ConstantSP set = conn.run(script);
    ASSERTION("testSet",set->size(),6);
}

void testMemoryTable(){
    string script;
    //模拟生成需要保存到内存表的数据
    VectorSP names = Util::createVector(DT_STRING,5,100);
    VectorSP dates = Util::createVector(DT_DATE,5,100);
    VectorSP prices = Util::createVector(DT_DOUBLE,5,100);
    for(int i = 0 ;i < 5;i++){
        names->set(i,Util::createString("name_"+std::to_string(i)));
        dates->set(i,Util::createDate(2010,1,i+1));
        prices->set(i,Util::createDouble(i*i));
    } 
    vector<string> allnames = {"names","dates","prices"};
    vector<ConstantSP> allcols = {names,dates,prices};
    conn.upload(allnames,allcols);//将数据上传到server
    script += "insert into tglobal values(names,dates,prices);"; //通过insert into 方法将数据保存到内存表中
    script += "select * from tglobal;";
    TableSP table = conn.run(script); 
    cout<<table->getString()<<endl;
}

TableSP createDemoTable(){
    vector<string> colNames = {"name","date","price"};
    vector<DATA_TYPE> colTypes = {DT_STRING,DT_DATE,DT_DOUBLE};
    int colNum = 3,rowNum = 3;
    ConstantSP table = Util::createTable(colNames,colTypes,rowNum,100);
    vector<VectorSP> columnVecs;
    for(int i = 0 ;i < colNum ;i ++)
        columnVecs.push_back(table->getColumn(i));
        
    for(int i =  0 ;i < rowNum; i++){
        columnVecs[0]->set(i,Util::createString("name_"+std::to_string(i)));
        columnVecs[1]->set(i,Util::createDate(2010,1,i+1));
        columnVecs[2]->set(i,Util::createDouble(i*i));
    }
    return table;
}
   

void testDiskTable(){
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    string script;
    script += "db=database(\"/home/psui/demoTable1\");";
    script += "tDiskGlobal.append!(mt);";
    script += "saveTable(db,tDiskGlobal,`dt);";
    script += "select * from tDiskGlobal;";
    TableSP result = conn.run(script); 
    cout<<result->getString()<<endl;
}

void testDFSTable(){
    string script;
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
    script += "tableName = `demoTable;";
    script += "database(dbPath).loadTable(tableName).append!(mt);";
    script += "tradTable= database(dbPath).loadTable(tableName);";
    script += "select * from tradTable;";
    TableSP result = conn.run(script); 
    cout<<result->getString()<<endl;
}

int main(){

    DBConnection::initialize();
    bool ret = conn.connect(hostName,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
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
    testMarics();
    testTable();
    testDictionary();
    testSet();
    //testMemoryTable();
    //testDiskTable();
    //testDFSTable();
    printTestResults();
    return 0;
}
