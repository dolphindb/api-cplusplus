#include "../include/DolphinDB.h"
#include "../include/Util.h"
#include "../include/BatchTableWriter.h"
#include "../include/MultithreadedTableWriter.h"
#include "unistd.h"
#include "assert.h"

using namespace dolphindb;
using namespace std;

const string hostName = "192.168.0.16";
const int port = 9002;

void testUploadandReadVector(int test_size){
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");

	int colNum = 24, rowNum = test_size;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);

	srand((int)time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<ConstantSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(rand()%10,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(rand()%19,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	// conn.upload("tab1",{tab1});
    conn.upload(colNamesVec1,columnVecs);
    for(int i=0;i<colNum;i++)
        VectorSP temp = conn.run("col"+to_string(i));

    conn.run("undef all;");
    conn.close();

	// string *s = new string[2000];

}


void testUploadandReadinMemoryTable(int test_size){
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");

	int colNum = 24, rowNum = test_size;
	vector<string> colNamesVec1;
	for (int i = 0; i < colNum; i++){
		colNamesVec1.emplace_back("col"+to_string(i));
	}
	vector<DATA_TYPE> colTypesVec1;
	colTypesVec1.emplace_back(DT_CHAR);
	colTypesVec1.emplace_back(DT_BOOL);
	colTypesVec1.emplace_back(DT_SHORT);
	colTypesVec1.emplace_back(DT_INT);
	colTypesVec1.emplace_back(DT_LONG);
	colTypesVec1.emplace_back(DT_DATE);
	colTypesVec1.emplace_back(DT_MONTH);
	colTypesVec1.emplace_back(DT_TIME);
	colTypesVec1.emplace_back(DT_MINUTE);
	colTypesVec1.emplace_back(DT_DATETIME);
	colTypesVec1.emplace_back(DT_SECOND);
	colTypesVec1.emplace_back(DT_TIMESTAMP);
	colTypesVec1.emplace_back(DT_NANOTIME);
	colTypesVec1.emplace_back(DT_NANOTIMESTAMP);
	colTypesVec1.emplace_back(DT_FLOAT);
	colTypesVec1.emplace_back(DT_DOUBLE);
	colTypesVec1.emplace_back(DT_STRING);
	colTypesVec1.emplace_back(DT_UUID);
	colTypesVec1.emplace_back(DT_IP);
	colTypesVec1.emplace_back(DT_INT128);
	colTypesVec1.emplace_back(DT_BLOB);
	colTypesVec1.emplace_back(DT_DATEHOUR);
	colTypesVec1.emplace_back(DT_DECIMAL32);
	colTypesVec1.emplace_back(DT_DECIMAL64);

	srand((int)time(NULL));
	TableSP tab1 = Util::createTable(colNamesVec1, colTypesVec1, rowNum, rowNum);
	vector<ConstantSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++){
		columnVecs.emplace_back(tab1->getColumn(i));

	}
	for (int i = 0; i < rowNum-1; i++){
		columnVecs[0]->set(i, Util::createChar(rand()%CHAR_MAX));
		columnVecs[1]->set(i, Util::createBool(rand()%2));
		columnVecs[2]->set(i, Util::createShort(rand()%SHRT_MAX));
		columnVecs[3]->set(i, Util::createInt(rand()%INT_MAX));
		columnVecs[4]->set(i, Util::createLong(rand()%LLONG_MAX));
		columnVecs[5]->set(i, Util::createDate(rand()%INT_MAX));
		columnVecs[6]->set(i, Util::createMonth(rand()%INT_MAX));
		columnVecs[7]->set(i, Util::createTime(rand()%INT_MAX));
		columnVecs[8]->set(i, Util::createMinute(rand()%1440));
		columnVecs[9]->set(i, Util::createDateTime(rand()%INT_MAX));
		columnVecs[10]->set(i, Util::createSecond(rand()%86400));
		columnVecs[11]->set(i, Util::createTimestamp(rand()%LLONG_MAX));
		columnVecs[12]->set(i, Util::createNanoTime(rand()%LLONG_MAX));
		columnVecs[13]->set(i, Util::createNanoTimestamp(rand()%LLONG_MAX));
		columnVecs[14]->set(i, Util::createFloat(rand()/float(RAND_MAX)));
		columnVecs[15]->set(i, Util::createDouble(rand()/double(RAND_MAX)));
		columnVecs[16]->set(i, Util::createString("str"+to_string(i)));
		columnVecs[17]->set(i, Util::parseConstant(DT_UUID,"5d212a78-cc48-e3b1-4235-b4d91473ee87"));	
		columnVecs[18]->set(i, Util::parseConstant(DT_IP,"192.0.0."+to_string(rand()%255)));
		columnVecs[19]->set(i, Util::parseConstant(DT_INT128,"e1671797c52e15f763380b45e841ec32"));
		columnVecs[20]->set(i, Util::createBlob("blob"+to_string(i)));
		columnVecs[21]->set(i, Util::createDateHour(rand()%INT_MAX));
		columnVecs[22]->set(i, Util::createDecimal32(rand()%10,rand()/float(RAND_MAX)));
		columnVecs[23]->set(i, Util::createDecimal64(rand()%19,rand()/double(RAND_MAX)));
	}
	for (int j = 0; j < colNum; j++)
		columnVecs[j]->setNull(rowNum-1);

	conn.upload("tab1",{tab1}); // upload table
	assert(conn.run("tab1.rows()")->getInt() == test_size);
    TableSP temp = conn.run("tab1"); // read table

    conn.run("undef all;");
    conn.close();

}


void testuploadandReadTableWithCompressTypes(int test_size){
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");
	srand((int)time(NULL));
    const int count = test_size;
    const int scale32=rand()%9, scale64=rand()%18;

    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> blob(count);
    vector<string> ipaddr(count);
    vector<double> decimal32(count);
    vector<double> decimal64(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        blob[i] = "fdsafsdfasd"+ to_string(i%32);
        ipaddr[i] = "192.168.100." + to_string(i%255);
        decimal32[i] = rand()/double(RAND_MAX);
        decimal64[i] = rand()/double(RAND_MAX);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP blobVector = Util::createVector(DT_BLOB, count,count);
    VectorSP decimal32Vector = Util::createVector(DT_DECIMAL32, count,count, true, scale32);
    VectorSP decimal64Vector = Util::createVector(DT_DECIMAL64, count,count, true, scale64);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    blobVector->setString(0,count,blob.data());
    decimal32Vector->setDouble(0,count,decimal32.data());
    decimal64Vector->setDouble(0,count,decimal64.data());

    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cblob","cdecimal32","cdecimal64"};
    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,blobVector,decimal32Vector,decimal64Vector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);
    string script = "colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cblob`cdecimal32`cdecimal64;\n"
                    "colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,BLOB,DECIMAL32("+to_string(scale32)+"),DECIMAL64("+to_string(scale64)+")];\n"
                    "share table(1:0,colName,colType) as table1;";
    conn.run(script);

    vector<ConstantSP> args{table};
    conn.run("tableInsert{table1}",args); // insert table
	assert(conn.run("table1.rows()")->getInt() == test_size);
    TableSP t1 = conn.run("select * from table1"); // read table

    conn.run("undef all;");
    conn.close();

}


void testAutoFitTableAppenderWithCompressedDatas(int test_size){
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");
	srand((int)time(NULL));
    const int count = test_size;
    const int scale32=rand()%9, scale64=rand()%18;

    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> ipaddr(count);
    vector<double> decimal32(count);
    vector<double> decimal64(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        ipaddr[i] = "192.168.100." + to_string(i%255);
        decimal32[i] = rand()/double(RAND_MAX);
        decimal64[i] = rand()/double(RAND_MAX);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP decimal32Vector = Util::createVector(DT_DECIMAL32, count,count, true, scale32);
    VectorSP decimal64Vector = Util::createVector(DT_DECIMAL64, count,count, true, scale64);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    decimal32Vector->setDouble(0,count,decimal32.data());
    decimal64Vector->setDouble(0,count,decimal64.data());

    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cdecimal32","cdecimal64"};
    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,decimal32Vector,decimal64Vector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);
    string script1;

    string dbName = "dfs://test_autoTableFitAppender";
    script1 += "dbName = \"dfs://test_autoTableFitAppender\";"
				"tableName=\"pt\";"
				"login(\"admin\",\"123456\");"
				"if(existsDatabase(dbName)){dropDatabase(dbName)};"
				"db=database(dbName,HASH,[INT,1]);"
				"colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cdecimal32`cdecimal64;"
				"colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,DECIMAL32("+to_string(scale32)+"),DECIMAL64("+to_string(scale64)+")];"
				"table1=table(1000:0,colName,colType);"
				"pt = db.createPartitionedTable(table1, `pt, `cint)";
    //cout<<script1<<endl;
    conn.run(script1);

    AutoFitTableAppender appender("dfs://test_autoTableFitAppender", "pt", conn);
	int rows = appender.append(table);
	assert(rows == test_size);

    TableSP t1 = conn.run("select * from loadTable(dbName,`pt)"); // read table

    conn.run("undef all;");
    conn.close();

}


void testAutoFitTableUpsertWithCompressedDatas(int test_size){
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");
	srand((int)time(NULL));
    const int count = test_size;
    const int scale32=rand()%9, scale64=rand()%18;

    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> ipaddr(count);
    vector<double> decimal32(count);
    vector<double> decimal64(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        ipaddr[i] = "192.168.100." + to_string(i%255);
        decimal32[i] = rand()/double(RAND_MAX);
        decimal64[i] = rand()/double(RAND_MAX);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP decimal32Vector = Util::createVector(DT_DECIMAL32, count,count, true, scale32);
    VectorSP decimal64Vector = Util::createVector(DT_DECIMAL64, count,count, true, scale64);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    decimal32Vector->setDouble(0,count,decimal32.data());
    decimal64Vector->setDouble(0,count,decimal64.data());

    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cdecimal32","cdecimal64"};
    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,decimal32Vector,decimal64Vector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);
    string script1;

    string dbName = "dfs://test_AutoFitTableUpsert";
    script1 += "dbName = \"dfs://test_AutoFitTableUpsert\";"
				"tableName=\"pt\";"
				"login(\"admin\",\"123456\");"
				"if(existsDatabase(dbName)){dropDatabase(dbName)};"
				"db=database(dbName,HASH,[INT,1]);"
				"colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cdecimal32`cdecimal64;"
				"colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,DECIMAL32("+to_string(scale32)+"),DECIMAL64("+to_string(scale64)+")];"
				"table1=table(1000:0,colName,colType);"
				"pt = db.createPartitionedTable(table1, `pt, `cint)";
    conn.run(script1);

    vector<string> keycolName = {"cint"};
    AutoFitTableUpsert aftu(dbName, "pt", conn, false, &keycolName);
	aftu.upsert(table);
	assert(conn.run("exec count(*) from loadTable(dbName,`pt)")->getInt() == test_size);

    TableSP t1 = conn.run("select * from loadTable(dbName,`pt)"); // read table

    conn.run("undef all;");
    conn.close();

}


void testPartitionedTableAppenderWithCompressedDatas(int test_size){
	DBConnectionPool pool(hostName, port, 2, "admin", "123456");
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");

	srand((int)time(NULL));
    const int count = test_size;
    const int scale32=rand()%9, scale64=rand()%18;

    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> ipaddr(count);
    vector<double> decimal32(count);
    vector<double> decimal64(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        ipaddr[i] = "192.168.100." + to_string(i%255);
        decimal32[i] = rand()/double(RAND_MAX);
        decimal64[i] = rand()/double(RAND_MAX);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP decimal32Vector = Util::createVector(DT_DECIMAL32, count,count, true, scale32);
    VectorSP decimal64Vector = Util::createVector(DT_DECIMAL64, count,count, true, scale64);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    decimal32Vector->setDouble(0,count,decimal32.data());
    decimal64Vector->setDouble(0,count,decimal64.data());

    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cdecimal32","cdecimal64"};
    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,decimal32Vector,decimal64Vector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);
    string script1;

    string dbName = "dfs://test_PartitionedTableAppender";
    script1 += "dbName = \"dfs://test_PartitionedTableAppender\";"
				"tableName=\"pt\";"
				"login(\"admin\",\"123456\");"
				"if(existsDatabase(dbName)){dropDatabase(dbName)};"
				"db=database(dbName,HASH,[INT,1]);"
				"colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cdecimal32`cdecimal64;"
				"colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,DECIMAL32("+to_string(scale32)+"),DECIMAL64("+to_string(scale64)+")];"
				"table1=table(1000:0,colName,colType);"
				"pt = db.createPartitionedTable(table1, `pt, `cint)";
    conn.run(script1);

    PartitionedTableAppender appender(dbName, "pt", "cint", pool);
    int res = appender.append(table);
	assert(conn.run("exec count(*) from loadTable(dbName,`pt)")->getInt() == test_size);

    TableSP t1 = conn.run("select * from loadTable(dbName,`pt)"); // read table

    conn.run("undef all;");
	pool.shutDown();
    conn.close();

}


void testMultithreadedTableWriterWithCompressedDatas(int test_size){
	DBConnection conn(false, false);
    conn.initialize();
    bool ret = conn.connect(hostName, port, "admin", "123456");
	srand((int)time(NULL));
    const int count = test_size;
    const int scale32=rand()%9, scale64=rand()%18;

    vector<int> time(count);
    vector<long long>value(count);
    vector<float> cfloat(count);
    vector<string> name(count);
    vector<string> ipaddr(count);
    vector<double> decimal32(count);
    vector<double> decimal64(count);
    int basetime = Util::countDays(2012,1,1);
    for(int i=0;i<count;i++){
        time[i] = basetime + (i%15);
        value[i] = (i % 500 == 0) ? i : (long long) nullptr;
        cfloat[i] = float(i)+0.1;
        name[i] = to_string(i);
        ipaddr[i] = "192.168.100." + to_string(i%255);
        decimal32[i] = rand()/double(RAND_MAX);
        decimal64[i] = rand()/double(RAND_MAX);
    }

    VectorSP boolVector = Util::createVector(DT_BOOL,count, count);
    VectorSP charVector = Util::createVector(DT_CHAR, count,count);
    VectorSP shortVector = Util::createVector(DT_SHORT,count,count);
    VectorSP intVector = Util::createVector(DT_INT,count,count);
    VectorSP dateVector = Util::createVector(DT_DATE,count,count);
    VectorSP monthVector = Util::createVector(DT_MONTH,count,count);
    VectorSP timeVector = Util::createVector(DT_TIME,count,count);
    VectorSP minuteVector = Util::createVector(DT_MINUTE,count,count);
    VectorSP secondVector = Util::createVector(DT_SECOND,count,count);
    VectorSP datetimeVector = Util::createVector(DT_DATETIME,count,count);
    VectorSP timestampVector = Util::createVector(DT_TIMESTAMP,count,count);
    VectorSP nanotimeVector = Util::createVector(DT_NANOTIME,count,count);
    VectorSP nanotimestampVector = Util::createVector(DT_NANOTIMESTAMP,count,count);
    VectorSP floatVector = Util::createVector(DT_FLOAT,count,count);
    VectorSP doubleVector = Util::createVector(DT_DOUBLE,count,count);
    VectorSP symbolVector = Util::createVector(DT_SYMBOL,count,count);
    VectorSP stringVector = Util::createVector(DT_STRING,count,count);
    VectorSP ipaddrVector = Util::createVector(DT_IP, count,count);
    VectorSP decimal32Vector = Util::createVector(DT_DECIMAL32, count,count, true, scale32);
    VectorSP decimal64Vector = Util::createVector(DT_DECIMAL64, count,count, true, scale64);

    boolVector->setInt(0,count,time.data());
    charVector->setInt(0,count,time.data());
    shortVector->setInt(0,count,time.data());
    intVector->setInt(0,count,time.data());
    dateVector->setInt(0,count,time.data());
    monthVector->setInt(0,count,time.data());
    timeVector->setInt(0,count,time.data());
    minuteVector->setInt(0,count,time.data());
    secondVector->setInt(0,count,time.data());
    datetimeVector->setLong(0,count,value.data());
    timestampVector->setLong(0,count,value.data());
    nanotimeVector->setLong(0,count,value.data());
    nanotimestampVector->setLong(0,count,value.data());
    floatVector->setFloat(0,count,cfloat.data());
    doubleVector->setFloat(0,count,cfloat.data());
    symbolVector->setString(0,count,name.data());
    stringVector->setString(0,count,name.data());
    ipaddrVector->setString(0,count,ipaddr.data());
    decimal32Vector->setDouble(0,count,decimal32.data());
    decimal64Vector->setDouble(0,count,decimal64.data());

    vector<string> colName={"cbool","cchar","cshort","cint","cdate","cmonth","ctime","cminute", "csecond","cdatetime","ctimestamp","cnanotime",
                            "cnanotimestamp","cfloat","cdouble","csymbol","cstring","cipaddr","cdecimal32","cdecimal64"};
    vector<ConstantSP> colVector {boolVector,charVector,shortVector,intVector,dateVector,monthVector,timeVector,minuteVector,secondVector,
                                  datetimeVector,timestampVector,nanotimeVector,nanotimestampVector,floatVector,doubleVector,symbolVector,stringVector,
                                  ipaddrVector,decimal32Vector,decimal64Vector};
    TableSP table  = Util::createTable(colName,colVector);
    vector<COMPRESS_METHOD> typeVec{COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,COMPRESS_DELTA,
                                    COMPRESS_DELTA,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,
                                    COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4,COMPRESS_LZ4};
    table->setColumnCompressMethods(typeVec);
    string script1;

    string dbName = "dfs://test_MultithreadedTableWriter";
    script1 += "dbName = \"dfs://test_MultithreadedTableWriter\";"
				"tableName=\"pt\";"
				"login(\"admin\",\"123456\");"
				"if(existsDatabase(dbName)){dropDatabase(dbName)};"
				"db=database(dbName,HASH,[INT,1]);"
				"colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cdecimal32`cdecimal64;"
				"colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,DECIMAL32("+to_string(scale32)+"),DECIMAL64("+to_string(scale64)+")];"
				"table1=table(1000:0,colName,colType);"
				"pt = db.createPartitionedTable(table1, `pt, `cint)";
    conn.run(script1);

    SmartPointer<MultithreadedTableWriter> mulwrite;
    ErrorCodeInfo pErrorInfo;
    mulwrite =new MultithreadedTableWriter(hostName, port, "admin","123456",dbName,"pt",NULL,false,NULL,1000,1,2,"cint");
	MultithreadedTableWriter::Status status;
    for(int i=0;i < table->rows();i++){
		mulwrite->insert(pErrorInfo, table->getColumn(0)->getRow(i),table->getColumn(1)->getRow(i),table->getColumn(2)->getRow(i),table->getColumn(3)->getRow(i),
		table->getColumn(4)->getRow(i),table->getColumn(5)->getRow(i),table->getColumn(6)->getRow(i),table->getColumn(7)->getRow(i),table->getColumn(8)->getRow(i),
		table->getColumn(9)->getRow(i),table->getColumn(10)->getRow(i),table->getColumn(11)->getRow(i),table->getColumn(12)->getRow(i),table->getColumn(13)->getRow(i),
		table->getColumn(14)->getRow(i),table->getColumn(15)->getRow(i),table->getColumn(16)->getRow(i),table->getColumn(17)->getRow(i),table->getColumn(18)->getRow(i),table->getColumn(19)->getRow(i));
	}

	mulwrite->waitForThreadCompletion();
	// cout<<pErrorInfo.errorInfo<<endl;
	assert(conn.run("exec count(*) from loadTable(dbName,`pt)")->getInt() == test_size);

    TableSP t1 = conn.run("select * from loadTable(dbName,`pt)"); // read table

    conn.run("undef all;");
    conn.close();

}

int main(int argc, char **argv){
	int index =0;
    double_t tm;
    const int table_rows = atoi(((string)(argv[1])).c_str());
    const int run_times = atoi(((string)(argv[2])).c_str());

    tm = Util::getEpochTime();
	cout<<"testUploadandReadVector started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
	{
		index++;
		try{
			testUploadandReadVector(table_rows);
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
		}
		catch(...){
			cout<<"unknow err occured in testUploadandReadVector(), please check....."<<endl;
		}
		sleep(1);
	}
	sleep(2);

	index =0;
    tm = Util::getEpochTime();
	cout<<"testUploadandReadinMemoryTable started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
	{
		index++;
		try{
			testUploadandReadinMemoryTable(table_rows);
		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
		}
		catch(...){
			cout<<"unknow err occured in testUploadandReadinMemoryTable(), please check....."<<endl;
		}
		sleep(1);
	}
	sleep(2);

	index =0;
    tm = Util::getEpochTime();
	cout<<"testuploadandReadTableWithCompressTypes started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
	{
		index++;
		try{
			testuploadandReadTableWithCompressTypes(table_rows);

		}
		catch(const std::exception& e)
		{
			std::cerr << e.what() << '\n';
		}
		catch(...){
			cout<<"unknow err occured in testuploadandReadTableWithCompressTypes(), please check....."<<endl;
		}
		sleep(1);
	}
	sleep(2);

	index =0;
    tm = Util::getEpochTime();
	cout<<"testAutoFitTableAppenderWithCompressedDatas started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
    {
        index++;
        try{
            testAutoFitTableAppenderWithCompressedDatas(table_rows);

        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...){
            cout<<"unknow err occured in testAutoFitTableAppenderWithCompressedDatas(), please check....."<<endl;
        }
	    sleep(1);
    }
	sleep(2);

	index =0;
    tm = Util::getEpochTime();
	cout<<"testAutoFitTableUpsertWithCompressedDatas started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
    {
        index++;
        try{
            testAutoFitTableUpsertWithCompressedDatas(table_rows);

        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...){
            cout<<"unknow err occured in testAutoFitTableUpsertWithCompressedDatas(), please check....."<<endl;
        }
	    sleep(1);
    }
	sleep(2);

	index =0;
    tm = Util::getEpochTime();
	cout<<"testPartitionedTableAppenderWithCompressedDatas started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
    {
        index++;
        try{
            testPartitionedTableAppenderWithCompressedDatas(table_rows);

        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...){
            cout<<"unknow err occured in testPartitionedTableAppenderWithCompressedDatas(), please check....."<<endl;
        }
	    sleep(1);
    }
	sleep(2);

	index =0;
    tm = Util::getEpochTime();
	cout<<"testMultithreadedTableWriterWithCompressedDatas started at: "<<Util::createTimestamp(tm)->getString()<<endl;
	while (index < run_times)
    {
        index++;
        try{
            testMultithreadedTableWriterWithCompressedDatas(table_rows);

        }
        catch(const std::exception& e)
        {
            std::cerr << e.what() << '\n';
        }
        catch(...){
            cout<<"unknow err occured in testMultithreadedTableWriterWithCompressedDatas(), please check....."<<endl;
        }
	    sleep(1);
    }
	return 0;
}