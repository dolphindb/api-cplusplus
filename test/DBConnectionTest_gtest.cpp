class DBConnectionTest:public testing::Test
{
protected:
    //Suite
    static void SetUpTestCase() {
        //DBConnection connReconn;
        connReconn.initialize();
        connReconn.connect(hostName, port, "admin", "123456", "", false, vector<string>(), 7200, true);
    }
    static void TearDownTestCase(){
        connReconn.close();
    }

    //Case
    virtual void SetUp()
    {
        cout<<"check connect...";
		ConstantSP res = connReconn.run("1+1");
		if(!(res->getBool())){
			cout<<"Server not responed, please check."<<endl;
		}
		else
		{
			cout<<"ok"<<endl;
            
		}
    }
    virtual void TearDown()
    {
        connReconn.run("undef all;");
    }
};

bool strToBool(string val){
    std::transform(val.begin(),val.end(),val.begin(),::tolower);
    vector<string> falsevec = {"false", "f", "", "0"};
    for(auto &i:falsevec){
        if(val == i)
            return false;
        else
            return true;
    }
    return NULL;
}

void StopCurNode(string cur_node){
    DBConnection conn1(false,false);
    conn1.connect(hostName, ctl_port, "admin", "123456");

    conn1.run("try{stopDataNode(\"" + cur_node + "\")}catch(ex){};");
    cout<< cur_node + " has stopped..."<<endl;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    // std::this_thread::yield();
    conn1.run("try{startDataNode(\"" + cur_node + "\")}catch(ex){};sleep(2000)");
    if(conn1.run("(exec state from getClusterPerf() where name = `"+cur_node+")[0]")->getInt() == 1){
        cout<< "restart the datanode: "+cur_node +" successfully..."<<endl;
        conn1.close();
        return;
    }
    else{
        cout<< "restart datanode failed."<<endl;
        conn1.close();
        return;
    }

}

bool assertUnConnect(){
    Util::sleep(2000);
    DBConnection conn2(false,false);
    cout <<"check if unconnected..."<<endl;
    return conn2.connect(hostName, port, "admin", "123456");
}

TEST_F(DBConnectionTest,test_connect_withErrUserid){
    DBConnection conn_demo(false,false);
    EXPECT_FALSE(conn_demo.connect(hostName, port, "adminasdvvv", "123456"));
}

TEST_F(DBConnectionTest,test_connect_withErrPassword){
    DBConnection conn_demo(false,false);
    EXPECT_FALSE(conn_demo.connect(hostName, port, "admin", "123456789"));
}

TEST_F(DBConnectionTest,test_connect_withStartupScript){
    string script = "tab=table(`1`2`3 as col1,4 5 6 as col2);share tab as startup_tab";
    DBConnection conn_demo(false,false);
    conn_demo.connect(hostName, port, "admin", "123456", script, false, vector<string>(), 7200, false);

    ConstantSP resVec = conn_demo.run("tab1=table(`1`2`3 as col1,4 5 6 as col2);each(eqObj,tab1.values(),startup_tab.values())");
    for(auto i=0; i<resVec->size(); i++)
        EXPECT_TRUE(resVec->get(i)->getBool());
    
    string res = conn_demo.getInitScript();
    EXPECT_EQ(script, res);
    conn_demo.close();

    string script2 = "tab=table(`1`2`3 as col1,4 5 6 as col2);share tab as startup_tab_2";
    conn_demo.setInitScript(script2);
    conn_demo.connect(hostName, port, "admin", "123456", script2, false, vector<string>(), 7200, false);
    EXPECT_EQ(conn_demo.getInitScript(),script2);

    conn_demo.run("undef(`startup_tab,SHARED);undef(`startup_tab_2,SHARED);");
    conn_demo.close();
}

TEST_F(DBConnectionTest,test_connection_enableSSL){
    bool https_conf = strToBool(connReconn.run("getConfig()[`enableHTTPS]")->getString());
    if(https_conf == false){
        cout<< "skip this case when server's 'enableHTTPS' is False."<<endl;
        EXPECT_EQ(1,1);
    }
    else{
        DBConnection conn_demo(true);
        conn_demo.connect(hostName, port);
        conn_demo.login("admin", "123456", true);
        conn_demo.run(
                "login(`admin,`123456)\n"
                "dbpath=\"dfs://tickHiggs\"\n"
                "db1 = database("", VALUE, 2000.01.01..2030.12.31)\n"
                "db2 = database("", HASH, [SYMBOL, 40])\n"
                "if(existsDatabase(dbpath)){dropDatabase(dbpath)}\n"
                "db=database(dbpath, COMPO, [db1,db2],,'TSDB','CHUNK')\n"
                "t=table(100:0, `InstrumentId`Exchange`Type`Timestamp`GlobalId`Price`Volume`BidId`AskId`LocalTime`MdSeqTime, [SYMBOL,CHAR,CHAR,NANOTIMESTAMP,LONG,DOUBLE,INT,LONG,LONG,NANOTIMESTAMP,NANOTIMESTAMP])\n"
                "db.createPartitionedTable(t,`ticks,`Timestamp`InstrumentId, sortColumns=`InstrumentId`Timestamp);go\n"
                "n=10\n"
                "symbols=symbol(string(1..4000));"
                "exchange=`1`2`3`4;"
                "type=`1`2`3`4;"
                "dates=datetimeAdd(2020.01.01,0..2,'d');"
                "InstrumentId=take(symbols,n);"
                "Exchange=take(char(exchange),n);"
                "Type=take(char(type),n);"
                "GlobalId=take(long(10000),n);"
                "Price=double(rand(100,n));"
                "Volume=rand(1000,n);"
                "BidId=take(long(10000),n);"
                "AskId=take(long(10000),n);go\n"
                "for (date in dates){"
                    "time=rand(timestamp(date)+0..(1000*60*60*23),n);"
                    "Timestamp=nanotimestamp(time);"
                    "LocalTime=Timestamp;"
                    "MdSeqTime=Timestamp;"
                    "tmp=table(InstrumentId,Exchange,Type,Timestamp,GlobalId,Price,Volume,BidId,AskId,LocalTime,MdSeqTime);"
                    "loadTable(dbpath,\"ticks\").append!(tmp);};"
                );
        ConstantSP tab = conn_demo.run("select * from loadTable(dbpath,`ticks)");
        EXPECT_EQ(tab->rows(),30);
        conn_demo.close();
    }
}

TEST_F(DBConnectionTest,test_connection_asyncTask){
    connReconn.run("try{undef(`tab, SHARED);}catch(ex){}");
    connReconn.run("t=table(`1`2`3 as col1, 1 2 3 as col2);"
                    "share t as tab;"
                    "records = [];"
                        );
    DBConnection conn_demo(false, true);
    conn_demo.connect(hostName, port, "admin","123456");

    conn_demo.run("for(i in 1..5){tableInsert(tab, string(i), i);sleep(1000)};");
    cout<<"async job is running";
    do{
        connReconn.run("records.append!(exec count(*) from tab)");
        cout<<".";
        Util::sleep(1000);
    } while (connReconn.run("exec count(*) from tab")->getInt() != 8);
    connReconn.run("records.append!(exec count(*) from tab)");

    EXPECT_TRUE(connReconn.run("a=records.pop!();eqObj(a,8)")->getBool());
  
    connReconn.run("undef(`tab, SHARED)");
    conn_demo.close();
    
}

TEST_F(DBConnectionTest,test_connection_function_login){
    DBConnection conn_demo(false, false);
    conn_demo.connect(hostName, port);
    EXPECT_ANY_THROW(conn_demo.login("admin123123", "123456", false));
    EXPECT_ANY_THROW(conn_demo.login("admin", "123456789", false));

    conn_demo.login("admin", "123456", false);
    cout<<conn_demo.run("getRecentJobs()")->getString();

    conn_demo.close();
    
}

TEST_F(DBConnectionTest,test_connection_python_script){
    ConstantSP server_version = connReconn.run("version()");
    string v = server_version->getString();
    if (v.find("2.10") != 0){
        cout<< "server version not matched: 2.10.xx, skip this case."<<endl;
        EXPECT_EQ(1,1);
    }
    else{
        string script1 = "import dolphindb as ddb\n"
                        "def list_append(testtype):\n"
                        "\ta= [testtype(1),testtype(2),testtype(3)]\n"
                        "\ta.append(testtype(4))\n"
                        "\ta.append(None)\n"
                        "\tassert a ==[testtype(1),testtype(2),testtype(3),testtype(4),None],'1'\n"
                        "\ndef test_list_append():\n"
                        "\ttypes=[int,long,short,float,double,char,bool,date,minute,month,second,datetime,timestamp,nanotime,nanotimestamp,datehour]\n"
                        "\tfor testtype in types:\n"
                            "\t\tlist_append(testtype)\n"
                            "\t\treturn True;\n"
                        "\n"
                        "def test_list_append_ipaddr_str_uuid():\n"
                        "\ta1= []\n"
                        "\ta2= []\n"
                        "\ta3= ['1','2','3']\n"
                        "\ta1.append(ipaddr(\"192.168.1.13\"))\n"
                        "\ta2.append(uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\"))\n"
                        "\ta3.append('4')\n"
                        "\tassert a1 == [ipaddr(\"192.168.1.13\")],'2'\n"
                        "\tassert a2==[uuid(\"9d457e79-1bed-d6c2-3612-b0d31c1881f6\")],'3'\n"
                        "\tassert a3==['1','2','3','4'],'4'\n"
                        "\treturn True;\n"
                        "test_list_append()\n"
                        "test_list_append_ipaddr_str_uuid()";
        // cout<<script1<<endl;

        DBConnection conn_demo(false,false,7200,false,true);
        conn_demo.connect(hostName, port, "admin", "123456");
        ConstantSP res = conn_demo.run(script1);
        // cout<< script1;
        // cout<< res->getString();
        EXPECT_EQ(res->getBool(), true);

        conn_demo.close();
    }

}

TEST_F(DBConnectionTest,test_connection_python_dataform){
    ConstantSP server_version = connReconn.run("version()");
    string v = server_version->getString();
    if (v.find("2.10") != 0){
        cout<< "server version not matched: 2.10.xx, skip this case."<<endl;
        EXPECT_EQ(1,1);
    }
    else{
        string script1 = "import dolphindb as ddb\n"
                        "a=[1,2,3]\n"
                        "b={1,2,3}\n"
                        "c={1:1,2:2}\n"
                        "d=(12,3,4)";
        DBConnection conn_demo(false,false,7200,false,true);
        conn_demo.connect(hostName, port, "admin", "123456");
        conn_demo.run(script1);

        EXPECT_EQ(conn_demo.run("type(a)")->getString(), "list"); 
        EXPECT_EQ(conn_demo.run("type(b)")->getString(), "set"); 
        EXPECT_EQ(conn_demo.run("type(c)")->getString(), "dict"); 
        EXPECT_EQ(conn_demo.run("type(d)")->getString(), "tuple"); 

        conn_demo.close();
    }
}

TEST_F(DBConnectionTest,test_connection_python_setInitscriptAndgetInitscript){
    ConstantSP server_version = connReconn.run("version()");
    string v = server_version->getString();
    if (v.find("2.10") != 0){
        cout<< "server version not matched: 2.10.xx, skip this case."<<endl;
        EXPECT_EQ(1,1);
    }
    else{
        string script1 = "import dolphindb as ddb";
        DBConnection conn_demo(false,false,7200,false,true);
        conn_demo.connect(hostName, port, "admin", "123456", script1);
        string res = conn_demo.getInitScript();
        // cout<< res;
        EXPECT_EQ(res, script1);

        conn_demo.close();
    }
}

TEST_F(DBConnectionTest,test_connection_python_upload){
    ConstantSP server_version = connReconn.run("version()");
    string v = server_version->getString();
    if (v.find("2.10") != 0){
        cout<< "server version not matched: 2.10.xx, skip this case."<<endl;
        EXPECT_EQ(1,1);
    }
    else{
        vector<string> colName = {"col1", "col2", "col3", "col4", "col5"};
        vector<DATA_TYPE> colType = {DT_BOOL, DT_INT, DT_STRING, DT_DATE, DT_FLOAT};
        TableSP t = Util::createTable(colName, colType, 5, 5);

        t->set(0,0,Util::createBool(1));
        t->set(1,0,Util::createInt(1));
        t->set(2,0,Util::createString("abc"));
        t->set(3,0,Util::createDate(1));
        t->set(4,0,Util::createFloat(1.123));

        DBConnection conn_demo(false,false,7200,false,true);
        conn_demo.connect(hostName, port, "admin", "123456");
        conn_demo.upload("t",{t});
        TableSP t1 = conn_demo.run("t");
        EXPECT_EQ(t1->getString(),t->getString());

        conn_demo.close();
    }
}

TEST_F(DBConnectionTest,test_connect_reconnect){
    #ifdef WINDOWS
        cout<<"skip this case."<<endl;
        EXPECT_EQ(1,1);
    #else
        bool res;
        string cur_node = connReconn.run("getNodeAlias()")->getString();

        std::thread t1= std::thread(StopCurNode, cur_node);
        std::thread t2= std::thread([&]{res = assertUnConnect();});

        t1.join();
        t2.join();

        Util::sleep(10000);
        EXPECT_EQ(res,false);
        cout <<"check passed..."<<endl;
        cout <<"check if reconnected..."<<endl;
        EXPECT_EQ(connReconn.run("1+1")->getInt(), 2);
        cout <<"check passed..."<<endl;
        // std::thread t1= std::thread(job);
        // t1.join();
    #endif
}

TEST_F(DBConnectionTest,test_connectionPool_withErrUserid){
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "adminasdccc", "123456"));
}

TEST_F(DBConnectionTest,test_connectionPool_withErrPassword){
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456789"));
}

TEST_F(DBConnectionTest,test_connectionPool_loadBalance){
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "adminasdccc", "123456", true));
    EXPECT_ANY_THROW(DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456789", true));
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456", true);
    DBConnection controller(false, false);
    controller.connect(hostName,ctl_port,"admin","123456");

    connReconn.run("login(`admin,`123456);"
                "dbpath=\"dfs://test_dfs\";"
                "if(existsDatabase(dbpath)){dropDatabase(dbpath)};"
                "db=database(dbpath, VALUE, 2000.01.01..2000.12.20);"
                "t=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);"
                "db.createPartitionedTable(t,`dfs_tab,`col3);");

    pool_demo.run("t=table(100:0, `col1`col2`col3, [SYMBOL,INT,DATE]);"
                    "tableInsert(t,rand(`APPLE`YSL`BMW`HW`SAUM`TDB,1000), rand(1000,1000), take(2000.01.01..2000.12.20,1000));"
                    "loadTable('dfs://test_dfs',`dfs_tab).append!(t);", 1000);
	while(!pool_demo.isFinished(1000)){
        // cout<<controller.run(
        //         "dnload=take(double(0),7);"
		// 		"for (i in 0..6){"
		// 		"nodeload =exec double(memoryUsed*0.3+connectionNum*0.4+cpuUsage*0.3) from  getClusterPerf() where name = \"datanode\"+string(i+1);"
		// 		"dnload[i]=nodeload[0]};"
		// 		"max(dnload)-min(dnload);")->getDouble()<<endl;
		Util::sleep(100);
	};

	TableSP ex_tab = connReconn.run("select * from loadTable('dfs://test_dfs',`dfs_tab)");
	EXPECT_EQ(1000, ex_tab->rows());
	EXPECT_FALSE(pool_demo.isShutDown());

    pool_demo.shutDown();
    controller.close();
}

TEST_F(DBConnectionTest,test_connectionPool_compress){
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456", false, false, true);
    const int count = 1000;
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
        decimal32[i] = 0.13566;
        decimal64[i] = 1.2245667899;
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
    connReconn.upload("table",table);

    vector<ConstantSP> args{table};
    string script = "colName =  `cbool`cchar`cshort`cint`cdate`cmonth`ctime`cminute`csecond`cdatetime`ctimestamp`cnanotime`cnanotimestamp`cfloat`cdouble`csymbol`cstring`cipaddr`cblob`cdecimal32`cdecimal64;\n"
                    "colType = [BOOL, CHAR, SHORT, INT, DATE, MONTH, TIME, MINUTE, SECOND, DATETIME, TIMESTAMP, NANOTIME, NANOTIMESTAMP, FLOAT, DOUBLE, SYMBOL, STRING,IPADDR,BLOB,DECIMAL32("+to_string(scale32)+"),DECIMAL64("+to_string(scale64)+")];\n"
                    "share streamTable(1:0,colName,colType) as table1;";
    connReconn.run(script);

    pool_demo.run("tableInsert{table1}",args,1000);
	while(!pool_demo.isFinished(1000)){
		Util::sleep(1000);
	};

    EXPECT_EQ(pool_demo.getData(1000)->getInt(), count);
    ConstantSP res = connReconn.run("each(eqObj,table.values(),table1.values())");
	for (int i = 0; i < res->size(); i++)
		EXPECT_TRUE(res->get(i)->getBool());

    connReconn.run("undef(`table1, SHARED)");
    pool_demo.shutDown();
}


TEST_F(DBConnectionTest,test_DBconnectionPool){
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456");
	EXPECT_EQ(pool_demo.getConnectionCount(), 10);
	vector<int> ids = {1, 10, 100, 1000};
	string srcipt1 = "tb = table(100:0,`col1`col2`col3,[INT,INT,INT]);share tb as tmp;";
	connReconn.run(srcipt1);
	EXPECT_ANY_THROW(pool_demo.run("for(i in 1..100){tableInsert(tmp,rand(100,1),rand(100,1),rand(100,1));};select * from tmp",-1));
	pool_demo.run("for(i in 1..100){tableInsert(tmp,rand(100,1),rand(100,1),rand(100,1));};select * from tmp",ids[0]);
	
	while(!pool_demo.isFinished(ids[0])){
		Util::sleep(1000);
	};

	TableSP ex_tab = connReconn.run("tmp");
	EXPECT_EQ(pool_demo.getData(ids[0])->getString(),ex_tab->getString());
	EXPECT_FALSE(pool_demo.isShutDown());

	vector<ConstantSP> args;
	args.push_back(ex_tab);
	pool_demo.run("tableInsert{tmp}",args,ids[1]);
	while(!pool_demo.isFinished(ids[1])){
		Util::sleep(1000);
	};

	pool_demo.run("exec * from tmp",ids[2]);
	while(!pool_demo.isFinished(ids[2])){
		Util::sleep(1000);
	};
	ex_tab = connReconn.run("tmp");
	EXPECT_EQ(pool_demo.getData(ids[2])->getString(),ex_tab->getString());
	EXPECT_ANY_THROW(pool_demo.getData(ids[2])); // id can only be used once.
	EXPECT_FALSE(pool_demo.isShutDown());

	connReconn.run("undef(`tmp,SHARED);");
    pool_demo.shutDown();
}

TEST_F(DBConnectionTest,test_DBconnectionPoolwithFetchSize){
    DBConnectionPool pool_demo(hostName, port, 10, "admin", "123456");
	EXPECT_EQ(pool_demo.getConnectionCount(), 10);
	vector<int> ids = {1, 10, 100, 1000};
	string srcipt1 = "tb = table(100:0,`col1`col2`col3,[INT,INT,INT]);share tb as tmp;";
	connReconn.run(srcipt1);
	pool_demo.run("for(i in 1..10000){tableInsert(tmp,rand(100,1),rand(100,1),rand(100,1));};select * from tmp",ids[1],4,2,2000); // fetchSize is useless
	
	while(!pool_demo.isFinished(ids[1])){
		Util::sleep(1000);
	};

	TableSP res = pool_demo.getData(ids[1]);
	EXPECT_EQ(res->rows(),10000);
	EXPECT_EQ(res->getForm(),DF_TABLE);

	TableSP ex_tab = connReconn.run("tmp");
	vector<ConstantSP> args;
	args.push_back(ex_tab);
	pool_demo.run("tableInsert{tmp}",args,ids[1],4,2,2000);

	while(!pool_demo.isFinished(ids[1])){
		Util::sleep(1000);
	};

	pool_demo.run("exec * from tmp",ids[2]);

	while(!pool_demo.isFinished(ids[2])){
		Util::sleep(1000);
	};

	TableSP res2 = pool_demo.getData(ids[2]);
	EXPECT_EQ(res2->rows(),20000);
	EXPECT_EQ(res2->getForm(),DF_TABLE);

	connReconn.run("undef(`tmp,SHARED);");
    pool_demo.shutDown();
}