#include "config.h"
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

void printTestResults(const string& fileName){
    FILE * fp = fopen(fileName.c_str(),"wb+");
    if(fp == NULL)
    	throw RuntimeException("Failed to open file [" + fileName + "].");
    vector<string> result;
    result.push_back("total:" + std::to_string(pass + fail) + "\n") ;
    result.push_back("pass:" + std::to_string(pass) + "\n");
    result.push_back("fail:" + std::to_string(fail) + "\n");
    for(unsigned int i = 0 ;i < result.size(); i++)
    	fwrite(result[i].c_str(), 1, result[i].size(),fp);
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


void testStringNullVector(int vecSize){
    vector<string> values(vecSize,"NULL");
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += "`" + values[i];
    ConstantSP result = conn.run(script);
    for(int i = 0; i < vecSize; i++){
        ASSERTION("testStringNullVector",result->getString(i),values[i]);
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


void testIntNullVector(int vecSize){
    vector<ConstantSP> values(vecSize,Util::createNullConstant(DT_INT));
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + values[i]->getString();
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testIntNullVector",result->getItem(i)->getString(),values[i]->getString());

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


void testDoubleNullVector(int vecSize){
    vector<ConstantSP> values(vecSize,Util::createNullConstant(DT_DOUBLE));
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + values[i]->getString();
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testDoubleNullVector",result->getString(),values[i]->getString());

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


void testDatenullVector(int vecSize){
    vector<ConstantSP> values(vecSize,Util::createNullConstant(DT_DATE));
    string script;
    for(int i = 0 ;i < vecSize; i++)
        script += " " + values[i]->getString();
    ConstantSP result = conn.run(script);
    for(int i = 0 ;i < vecSize; i++)
        ASSERTION("testDateNullVector",result->getItem(i)->getString(),values[i]->getString());
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
    vector<long long> testValues = {1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000};
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


void testnanotimeVector(){
    string beginNanotime = "13:30:10.008007006";
    vector<long long> testValues = {1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000};
    vector<string> expectResults = {"13:30:10.008007007","13:30:10.008007016","13:30:10.008007106",
                                    "13:30:10.008008006","13:30:10.008017006","13:30:10.008107006","13:30:10.009007006",
                                    "13:30:10.018007006","13:30:10.108007006","13:30:11.008007006","13:30:20.008007006",
                                    "13:31:50.008007006","13:46:50.008007006"};
    string script;
    for(unsigned int i = 0; i < testValues.size(); i++){
        script += " " + std::to_string(testValues[i]);
    }
    script = beginNanotime + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0; i < testValues.size(); i++){
        ASSERTION("testnanotimeVector",result->getString(i),expectResults[i]);
    }
}


void testnanotimestampVector(){
    string beginNanotimestamp = "2012.06.13T13:30:10.008007006";
    vector<long long> testValues = {1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000,10000000000000,100000000000000,1000000000000000,10000000000000000,100000000000000000};
    vector<string> expectResults = {"2012.06.13T13:30:10.008007007","2012.06.13T13:30:10.008007016","2012.06.13T13:30:10.008007106",
                                    "2012.06.13T13:30:10.008008006","2012.06.13T13:30:10.008017006","2012.06.13T13:30:10.008107006","2012.06.13T13:30:10.009007006",
                                    "2012.06.13T13:30:10.018007006","2012.06.13T13:30:10.108007006","2012.06.13T13:30:11.008007006","2012.06.13T13:30:20.008007006",
                                    "2012.06.13T13:31:50.008007006","2012.06.13T13:46:50.008007006","2012.06.13T16:16:50.008007006","2012.06.14T17:16:50.008007006","2012.06.25T03:16:50.008007006","2012.10.07T07:16:50.008007006","2015.08.14T23:16:50.008007006"};
    string script;
    for(unsigned int i = 0; i < testValues.size(); i++){
        script += " " + std::to_string(testValues[i]);
    }
    script = beginNanotimestamp + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0; i < testValues.size(); i++){
        ASSERTION("testnanotimestampVector",result->getString(i),expectResults[i]);
    }
}


void testmonthVector(){
    string beginmonth="2012.06M";
    vector<int> testValues = {1,10,100,1000};
    vector<string> expectResults = {"2012.07M","2013.04M","2020.10M","2095.10M"};
    string script;
    for(unsigned int i = 0; i < testValues.size(); i++){
        script += " " + std::to_string(testValues[i]);
    }
    script = beginmonth + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0; i < testValues.size(); i++){
        ASSERTION("testmonthVector",result->getString(i),expectResults[i]);
    }
}

void testtimeVector(){
    string begintime="13:30:10.008";
    vector<int> testValues = {1,10,100,1000,10000,100000,1000000,10000000};
    vector<string> expectResults = {"13:30:10.009","13:30:10.018","13:30:10.108","13:30:11.008","13:30:20.008","13:31:50.008","13:46:50.008","16:16:50.008"};
    string script;
    for(unsigned int i = 0; i < testValues.size(); i++){
        script += " " + std::to_string(testValues[i]);
    }
    script = begintime + " + " + script;
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0; i < testValues.size(); i++){
        ASSERTION("testtimeVector",result->getString(i),expectResults[i]);
    }
}

void testSymbol(){
    vector<string> expectResults = {"XOM","y"};
    string script;
    script += "x=`XOM`y;y=symbol x;y;";
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < expectResults.size(); i++){
        ASSERTION("testSymbol",result->getString(i),expectResults[i]);
    }
}


void testmixtimevectorUpload(){
    VectorSP dates = Util::createVector(DT_ANY,5,100);
    dates->set(0, Util::createMonth(2016,6));
    dates->set(1,Util::createDate(2016,5,16));
    dates->set(2,Util::createDateTime(2016,6,6,6,12,12));
    dates->set(3,Util::createNanoTime(6,28,36,00));
    dates->set(4,Util::createNanoTimestamp(2020,8,20,2,20,20,00));
    vector<ConstantSP> mixtimedata = {dates};
    vector<string> mixtimename={"Mixtime"};
    conn.upload(mixtimename,mixtimedata);
}

void testFunctionDef(){
    string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
    ConstantSP result = conn.run(script);
    ASSERTION("testFunctionDef",result->getString(),string("300"));
}

void testMatrix(){
    vector<string> expectResults = {"{1,2}","{3,4}","{5,6}"};
    string script = "1..6$2:3";
    ConstantSP result = conn.run(script);
    for(unsigned int i = 0 ;i < expectResults.size(); i++){
        ASSERTION("testMatrix",result->getString(i),expectResults[i]);
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
    //simulation to generate data to be saved to the memory table
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
    conn.upload(allnames,allcols);//upload data to server
    script += "tglobal=table(names,dates,prices);";
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://demodb2\";";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "db=database(dbPath,VALUE,2010.01.01..2010.01.30);";
    script += "pt=db.createPartitionedTable(tglobal,`pt,`dates);";
    script += "pt.append!(tglobal);";
    script += "dropPartition(db,2010.01.01);";
    //script += "dropTable(db,`tglobal);";
    //script += "dropDatabase(\"dfs://demodb2\");";
    //script += "existsDatabase(\"dfs://demodb2\");";
    //script += "insert into tglobal values(names,dates,prices);";
    script += "select * from pt;";
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
    string dbPath = conn.run("getHomeDir()+\"/cpp_test\" ")->getString();
    string script;
    script += "dbPath;";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "db=database(dbPath,VALUE,2010.01.01..2010.01.30);";
    script += "tDiskGlobal=db.createPartitionedTable(mt,`tDiskGlobal,`date);";
    script += "tDiskGlobal.append!(mt);";
    //script += "saveTable(db,tDiskGlobal,`tDiskGlobal);";
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
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "tableName = `demoTable;";
    script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
    script += "date = db.createPartitionedTable(mt,tableName,`date);";
    //script += "date.tableInsert(mt);";
    script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
    //script += "tradTable= database(dbPath).loadTable(tableName);";
    //script += "dropPartition(db,2010.01.01);";
    //script += "dropTable(db,`demoTable);";
    //script += "existsTable(\"dfs://SAMPLE_TRDDB\",`demoTable);";
    //script += "dropDatabase(\"dfs://SAMPLE_TRDDB\");";
    //script += "existsDatabase(\"dfs://SAMPLE_TRDDB\");";
    script += "select * from date;";
    //script += "select * from date where date>2020.01;";
    TableSP result = conn.run(script); 
    cout<<result->getString()<<endl;
}


void testDimensionTable(){
    string script;
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://db1\";";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
    script += "dt = db.createTable(mt,`dt).append!(mt);";
    script += "select * from dt;";
    TableSP result = conn.run(script);
    cout<<result->getString()<<endl;
}


void ASSERTIONHASH(const string test, const int ret[], const int expect[],int len) {
	bool equal=true;
	for(int i=0;i<len;i++){
		if(ret[i]!=expect[i])
			equal=false;
	}

    if(!equal){
        std::cout<<"ASSERT FAIL--" << test<< "-- expect return --";
    	for(int i=0;i<len;i++)
    		std::cout<<expect[i]<<",";
    	cout<<", real return--";
    	for(int i=0;i<len;i++)
    		std::cout<<ret[i]<<",";
    	cout<< std::endl;
        fail++;
    }
    else
        pass++;

}
void testCharVectorHash(){
    vector<char> testValues{127,-127,12,0,-12,-128};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{10,12,12,0,10,-1},
    		{41,18,12,0,4,-1},
    		{56,24,12,0,68,-1},
    		{30,5,12,0,23,-1},
    		{127,129,12,0,244,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createChar(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testCharVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_CHAR,0);
    v->appendChar(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testCharVectorHash",hv,expected[j],6);
    }
}

void testShortVectorHash(){
    vector<short> testValues{32767,-32767,12,0,-12,-32768};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{7,2,12,0,10,-1},
    		{1,15,12,0,4,-1},
    		{36,44,12,0,68,-1},
    		{78,54,12,0,23,-1},
    		{4088,265,12,0,244,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createShort(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_SHORT,0);
    v->appendShort(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
}
void testIntVectorHash(){
    vector<int> testValues{INT_MAX,INT_MAX*(-1),12,0,-12,INT_MIN};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{10,12,12,0,10,-1},
    		{7,9,12,0,4,-1},
    		{39,41,12,0,68,-1},
    		{65,67,12,0,23,-1},
    		{127,129,12,0,244,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createInt(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testIntVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_INT,0);
    v->appendInt(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testIntVectorHash",hv,expected[j],6);
    }
}

void testLongVectorHash(){
    vector<long long> testValues{LLONG_MAX,(-1)*LLONG_MAX,12,0,-12,LLONG_MIN};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
    		{7,9,12,0,4,-1},
    		{41,0,12,0,29,-1},
    		{4,6,12,0,69,-1},
    		{78,80,12,0,49,-1},
    		{4088,4090,12,0,4069,-1}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createLong(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testLongVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_LONG,0);
    v->appendLong(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testLongVectorHash",hv,expected[j],6);
    }
}


void testStringVectorHash(){
    vector<string> testValues{"9223372036854775807","helloworldabcdefghijklmnopqrstuvwxyz","智臾科技","hello,智臾科技","123abc您好！",""};
    int buckets[5]={13,43,71,97,4097};
    int expected[5][6]={
            {8,1,11,8,10,0},
            {37,20,14,23,41,0},
            {31,0,41,63,40,0},
            {24,89,51,54,42,0},
            {739,3737,814,3963,3488,0}};
    int hv[6]={0};

    for(unsigned int j=0;j<5;j++){
		for(unsigned int i = 0 ;i < testValues.size(); i++){
			ConstantSP val=Util::createString(testValues[i]);
			hv[i]=val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_STRING,0);
    v->appendString(testValues.data(),testValues.size());
    for(unsigned int j=0;j<5;j++){
		v->getHash(0,6,buckets[j],hv);
		ASSERTIONHASH("testShortVectorHash",hv,expected[j],6);
    }
}
void testUUIDvectorHash(){
    string script;
    script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5]={13,43,71,97,4097};
    int hv[6]={0};
    int expected[5][6]={0};

    for(unsigned int j=0;j<5;j++){
 		for( int i = 0 ;i < t->size(); i++){
 			ConstantSP val=Util::parseConstant(DT_UUID,t->getColumn(0)->getString(i));
 			hv[i]=val->getHash(buckets[j]);
 			expected[j][i]=t->getColumn(j+1)->getInt(i);
 		}
 		ASSERTIONHASH("testUUIDvectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_UUID,0);
    for( int i = 0 ;i < t->size(); i++)
    	v->append(Util::parseConstant(DT_UUID,t->getColumn(0)->getString(i)));
    for(unsigned int j=0;j<5;j++){
 		v->getHash(0,6,buckets[j],hv);
 		ASSERTIONHASH("testUUIDvectorHash",hv,expected[j],6);
    }

}
void testIpAddrvectorHash(){
    string script;
    script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5]={13,43,71,97,4097};
    int hv[6]={0};
    int expected[5][6]={0};

    for(unsigned int j=0;j<5;j++){
 		for( int i = 0 ;i < t->size(); i++){
 			ConstantSP val=Util::parseConstant(DT_IP,t->getColumn(0)->getString(i));
 			hv[i]=val->getHash(buckets[j]);
 			expected[j][i]=t->getColumn(j+1)->getInt(i);
 		}
 		ASSERTIONHASH("testIpAddrvectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_IP,0);
    for( int i = 0 ;i < t->size(); i++)
        v->append(Util::parseConstant(DT_IP,t->getColumn(0)->getString(i)));
    for(unsigned int j=0;j<5;j++){
 		v->getHash(0,6,buckets[j],hv);
 		ASSERTIONHASH("testIpAddrvectorHash",hv,expected[j],6);
    }

}
void testInt128vectorHash(){
    string script;
    script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
    TableSP t = conn.run(script);

    int buckets[5]={13,43,71,97,4097};
    int hv[6]={0};
    int expected[5][6]={0};

    for(unsigned int j=0;j<5;j++){
 		for( int i = 0 ;i < t->size(); i++){
 			ConstantSP val=Util::parseConstant(DT_INT128,t->getColumn(0)->getString(i));
 			hv[i]=val->getHash(buckets[j]);
 			expected[j][i]=t->getColumn(j+1)->getInt(i);
 		}
 		ASSERTIONHASH("testInt128vectorHash",hv,expected[j],6);
    }
    VectorSP v=Util::createVector(DT_INT128,0);
    for( int i = 0 ;i < t->size(); i++)
        v->append(Util::parseConstant(DT_INT128,t->getColumn(0)->getString(i)));
    for(unsigned int j=0;j<5;j++){
 		v->getHash(0,6,buckets[j],hv);
 		ASSERTIONHASH("testInt128vectorHash",hv,expected[j],6);
    }
}


void testshare(){
    string dbPath = conn.run(" getHomeDir()+\"/cpp_test\"")->getString();
    string script;
    script += "TickDB = database(dbPath, RANGE, `A`M`ZZZZ, `DFS_NODE1`DFS_NODE2);";
    script += "t=table(rand(`AAPL`IBM`C`F,100) as sym, rand(1..10, 100) as qty, rand(10.25 10.5 10.75, 100) as price);";
    script += "share t as TickDB.Trades on sym;";
    //script += "dropTable(TickDB,`TickDB.Trades);";
    script += "select top 10 * from TickDB.Trades;";

    //script += "select count(*) from TickDB.Trades;";
    TableSP result = conn.run(script);
    cout<<result->getString()<<endl;

}

void testRun(){
    //所有参数都在服务器端
    /*conn.run("x = [1, 3, 5]; y = [2, 4, 6]");
    ConstantSP result = conn.run("add(x,y)");
    cout<<result->getString()<<endl;*/
    //仅有一个参数在服务器端
    /*conn.run("x = [1, 3, 5]");
    vector<ConstantSP> args;
    ConstantSP y = Util::createVector(DT_DOUBLE, 3);
    double array_y[] = {1.5, 2.5, 7};
    y->setDouble(0, 3, array_y);
    args.push_back(y);
    ConstantSP result = conn.run("add{x,}", args);
    cout<<result->getString()<<endl;*/
    //两个参数都在客户端
    vector<ConstantSP> args;
    ConstantSP x = Util::createVector(DT_DOUBLE, 3);
    double array_x[] = {1.5, 2.5, 7};
    x->setDouble(0, 3, array_x);
    ConstantSP y = Util::createVector(DT_DOUBLE, 3);
    double array_y[] = {8.5, 7.5, 3};
    y->setDouble(0, 3, array_y);
    args.push_back(x);
    args.push_back(y);
    ConstantSP result = conn.run("add", args);
    cout<<result->getString()<<endl;
}

void tets_Block_Reader_DFStable(){
    string script;
    string script1;
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://TEST_BLOCK\";";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "tableName = `pt;";
    script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
    script += "pt = db.createPartitionedTable(mt,tableName,`date);";
    script += "pt.append!(mt);";
    script += "n=12450;";
    script += "t1=table(take(`name_3,n) as name,take(2010.01.01,n) as date,rand(30,n) as price);";
    script += "pt.append!(t1);";
    conn.run(script);
    script1 += "select * from pt;";
    int fetchsize1 = 12453;
    BlockReaderSP reader = conn.run(script1,4,2,fetchsize1);
    ConstantSP t;
    int total = 0;
    while(reader->hasNext()){
        t=reader->read();
        total += t->size();
        //cout<< "read" <<t->size()<<endl;
        ASSERTION("tets_Block_Reader_DFStable",t->size(),fetchsize1);
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("tets_Block_Reader_DFStable",total,12453);

    int fetchsize2=8200;
    BlockReaderSP reader2 = conn.run(script1,4,2,fetchsize2);
    ConstantSP t2;
    int total2 = 0;
    int tmp = fetchsize2;
    while(reader2->hasNext()){
        t2=reader2->read();
        total2 += t2->size();
        //cout<< "read" <<t2->size()<<endl;
        ASSERTION("tets_Block_Reader_DFStable",t2->size(),tmp);
        tmp = 12453 - tmp;
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("tets_Block_Reader_DFStable",total2,12453);

    int fetchsize3=15000;
    BlockReaderSP reader3 = conn.run(script1,4,2,fetchsize3);
    ConstantSP t3;
    int total3 = 0;
    while(reader3->hasNext()){
        t3=reader3->read();
        total3 += t3->size();
        //cout<< "read" <<t2->size()<<endl;
        ASSERTION("tets_Block_Reader_DFStable",t3->size(),12453);
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("tets_Block_Reader_DFStable",total3,12453);
}

void test_Block_Table(){
    string script;
    string script1;
    script += "rows=12453;";
    script += "testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);";
    //script += "select count(*) from testblock;";
    conn.run(script);
    script1 += "select * from testblock ";
    int fetchsize1 = 12453;
    BlockReaderSP reader = conn.run(script1,4,2,fetchsize1);
    ConstantSP t;
    int total = 0;
    while(reader->hasNext()){
        t=reader->read();
        total += t->size();
        //cout<< "read" <<t->size()<<endl;
        ASSERTION("test_Block_Table",t->size(),fetchsize1);
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("test_Block_Table",total,12453);

    int fetchsize2=8200;
    BlockReaderSP reader2 = conn.run(script1,4,2,fetchsize2);
    ConstantSP t2;
    int total2 = 0;
    int tmp = fetchsize2;
    while(reader2->hasNext()){
        t2=reader2->read();
        total2 += t2->size();
        //cout<< "read" <<t2->size()<<endl;
        ASSERTION("test_Block_Table",t2->size(),tmp);
        tmp = 12453 - tmp;
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("test_Block_Table",total2,12453);

    int fetchsize3=15000;
    BlockReaderSP reader3 = conn.run(script1,4,2,fetchsize3);
    ConstantSP t3;
    int total3 = 0;
    while(reader3->hasNext()){
        t3=reader3->read();
        total3 += t3->size();
        //cout<< "read" <<t2->size()<<endl;
        ASSERTION("test_Block_Table",t3->size(),12453);
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("test_Block_Table",total3,12453);
}

void test_block_skipALL(){
    string script;
    script += "login(`admin,`123456);";
    script += "select * from loadTable(\"dfs://TEST_BLOCK\",\"pt\");";
    BlockReaderSP reader = conn.run(script,4,2,8200);
    ConstantSP t = reader->read();
    reader->skillAll();
    TableSP result = conn.run("table(1..100 as id1)");
    //cout<<result->getString()<<endl;
    ASSERTION("test_block_skipALL",result->size(),100);

}

void test_huge_table(){
    string script;
    string script1;
    script += "rows=20000000;";
    script += "testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);";
    conn.run(script);
    script1 += "select * from testblock;";
    BlockReaderSP reader = conn.run(script1,4,2,8200);
    ConstantSP t;
    int total = 0;
    int i= 1;
    int fetchsize =8200;
    while(reader->hasNext()){
        t=reader->read();
        total += t->size();
        //cout<< "read" <<t->size()<<endl;

        if(t->size() == 8200){
            ASSERTION("test_huge_table",t->size(),8200);
        }
        else {
            ASSERTION("test_huge_table",t->size(),200);
        }

    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("test_huge_table",total,20000000);
}


void test_huge_DFS(){
    string script;
    string script1;
    TableSP table = createDemoTable();
    conn.upload("mt",table);
    script += "login(`admin,`123456);";
    script += "dbPath = \"dfs://TEST_Huge_BLOCK\";";
    script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
    script += "tableName = `pt;";
    script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
    script += "pt = db.createPartitionedTable(mt,tableName,`date);";
    script += "pt.append!(mt);";
    script += "n=20000000;";
    script += "t1=table(take(`name_3,n) as name,take(2010.01.01,n) as date,rand(30,n) as price);";
    script += "pt.append!(t1);";
    conn.run(script);
    script1 += "select * from pt;";
    int fetchsize1 = 8200;
    BlockReaderSP reader = conn.run(script1,4,2,fetchsize1);
    ConstantSP t;
    int total = 0;
    while(reader->hasNext()){
        t=reader->read();
        total += t->size();
        //cout<< "read" <<t->size()<<endl;
        if(t->size() == 8200){
            ASSERTION("test_huge_DFS",t->size(),8200);
        }
        else {
            ASSERTION("test_huge_DFS",t->size(),203);
        }

    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("test_huge_DFS",total,20000003);

    int fetchsize2 = 2000000;
    BlockReaderSP reader2 = conn.run(script1,4,2,fetchsize2);
    ConstantSP t2;
    int total2 = 0;
    while(reader2->hasNext()){
        t2=reader2->read();
        total2 += t2->size();
        //cout<< "read" <<t2->size()<<endl;

        if(t2->size() == 2000000){
            ASSERTION("test_huge_DFS",t2->size(),2000000);
        }
        else {
            ASSERTION("test_huge_DFS",t2->size(),3);
        }

        //ASSERTION("tets_Block_Reader_DFStable",t->size(),fetchsize1);
    }
    //cout<<"total is"<<total<<endl;
    ASSERTION("test_huge_DFS",total2,20000003);

}

int main(int argc, char ** argv){
    DBConnection::initialize();
    bool ret = conn.connect(hostName,port);
    if(!ret){
		cout<<"Failed to connect to the server"<<endl;
		return 0;
	}
    int testVectorSize = 20;

    testStringVector(testVectorSize);
    testStringNullVector(testVectorSize);
    testIntVector(testVectorSize);
    testIntNullVector(testVectorSize);
    testDoubleVector(testVectorSize);
    testDoubleNullVector(testVectorSize);
    testDateVector();
    testDatenullVector(testVectorSize);
    testDatetimeVector();
    testTimeStampVector();
    testnanotimeVector();
    testnanotimestampVector();
    testmonthVector();
    testtimeVector();
    testFunctionDef();
    testMatrix();
    testTable();
    testDictionary();
    testSet();
    testSymbol();
    testmixtimevectorUpload();
   // testMemoryTable();
   // testDFSTable();
   // testDiskTable();
    testDimensionTable();
    testCharVectorHash();
    testShortVectorHash();
    testIntVectorHash();
    testLongVectorHash();
    testStringVectorHash();
    testUUIDvectorHash();
    testIpAddrvectorHash();
    testInt128vectorHash();
    testRun();
    //testshare();
    tets_Block_Reader_DFStable();
    test_Block_Table();
    test_block_skipALL();
    test_huge_table();
    test_huge_DFS();
    std::thread t1(test_Block_Table);
    t1.join();
    std::thread t2(tets_Block_Reader_DFStable);
    t2.join();
    if (argc >= 2) {
        string fileName(argv[1]);
        printTestResults(fileName);
    }

    return 0;
}
