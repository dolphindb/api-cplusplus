#include "config.h"
#include <thread>
#include "DolphinDB.h"

int64_t getTimeStampMs() {
	return Util::getEpochTime();
}

string genRandString(int maxSize) {
	string result;
	int size = rand() % maxSize;
	for (int i = 0;i < size;i++) {
		int r = rand() % alphas.size();
		result += alphas[r];
	}
	return result;
}

template<typename T>
void ASSERTION(const string& test, const T& ret, const T& expect) {
	if (ret != expect) {
		std::cout << "ASSERT FAIL--" << test << "-- expect return --" << (T)expect << ", real return--" << (T)ret << std::endl;
		fail++;
	}
	else
		std::cout << "ASSERT PASSED--" << test << std::endl;
	pass++;
}

static string getSymbolVector(const string& name, int size)
{
	int kind = 50;
	int count = size / kind;

	string result;
	char temp[200];
	result += name;
	sprintf(temp, "=symbol(array(STRING,%d,%d,`0));", count, count);
	result += temp;
	for (int i = 1;i<kind;i++) {
		sprintf(temp, ".append!(symbol(array(STRING,%d,%d,`%d)));", count, count, i);
		result += name;
		result += string(temp);
	}

	return result;
}

void printTestResults(const string& fileName) {
	FILE * fp = fopen(fileName.c_str(), "wb+");
	if (fp == nullptr)
		throw RuntimeException("Failed to open file [" + fileName + "].");
	vector<string> result;
	result.push_back("total:" + std::to_string(pass + fail) + "\n");
	result.push_back("pass:" + std::to_string(pass) + "\n");
	result.push_back("fail:" + std::to_string(fail) + "\n");
	for (auto & i : result)
		fwrite(i.c_str(), 1, i.size(), fp);
}


void testStringVector(int vecSize) {
	vector<string> values;
	values.reserve(vecSize);
	for (int i = 0;i < vecSize; i++)
		values.push_back(genRandString(30));
	string script;
	for (int i = 0;i < vecSize; i++)
		script += "`" + values[i];
	ConstantSP result = conn.run(script);
	for (int i = 0; i < vecSize; i++) {
		ASSERTION("testStringVector", result->getString(i), values[i]);
	}
}


void testStringNullVector(int vecSize) {
	vector<string> values(vecSize, "NULL");
	string script;
	for (int i = 0;i < vecSize; i++)
		script += "`" + values[i];
	ConstantSP result = conn.run(script);
	for (int i = 0; i < vecSize; i++) {
		ASSERTION("testStringNullVector", result->getString(i), values[i]);
	}
}


void testIntVector(int vecSize) {
	vector<int> values;
	values.reserve(vecSize);
	for (int i = 0;i < vecSize; i++)
		values.push_back(rand() % INT_MAX);
	string script;
	for (int i = 0;i < vecSize; i++)
		script += " " + std::to_string(values[i]);
	ConstantSP result = conn.run(script);
	for (int i = 0;i < vecSize; i++)
		ASSERTION("testIntVector", result->getInt(i), values[i]);
}


void testIntNullVector(int vecSize) {
	vector<ConstantSP> values(vecSize, Util::createNullConstant(DT_INT));
	string script;
	for (int i = 0;i < vecSize; i++)
		script += " " + values[i]->getString();
	ConstantSP result = conn.run(script);
	for (int i = 0;i < vecSize; i++)
		ASSERTION("testIntNullVector", result->getItem(i)->getString(), values[i]->getString());

}


void testDoubleVector(int vecSize) {
	vector<double> values;
	values.reserve(vecSize);
	for (int i = 0;i < vecSize; i++)
		values.push_back((double)(rand()));
	string script;
	for (int i = 0;i < vecSize; i++)
		script += " " + std::to_string(values[i]);
	ConstantSP result = conn.run(script);
	for (int i = 0;i < vecSize; i++)
		ASSERTION("testDoubleVector", result->getDouble(i), values[i]);
}


void testDoubleNullVector(int vecSize) {
	vector<ConstantSP> values(vecSize, Util::createNullConstant(DT_DOUBLE));
	string script;
	for (int i = 0;i < vecSize; i++)
		script += " " + values[i]->getString();
	ConstantSP result = conn.run(script);
	for (int i = 0;i < vecSize; i++)
		ASSERTION("testDoubleNullVector", result->getString(), values[i]->getString());

}
void testDatehourVector() {
	string script;
	string beginDatehour = "datehour(2021.05.01 10:10:10)";
	vector<int> testValues = { 1,10,100,1000,10000,100000 };
	vector<string> expectResults = { "2021.05.01T11","2021.05.01T20","2021.05.05T14","2021.06.12T02","2022.06.22T02","2032.09.27T02" };
	for (int testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginDatehour + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < testValues.size(); ++i) {
		ASSERTION("testDatehourVector", result->getString(i), expectResults[i]);
	}
}

void testDatehourNullVector(int vecSize) {
	vector<ConstantSP> values(vecSize, Util::createNullConstant(DT_DATEHOUR));
	string script;
	for (int i = 0;i<vecSize;i++)
		script += " " + values[i]->getString();
	ConstantSP result = conn.run(script);
	for (int i = 0;i < vecSize; i++)
		ASSERTION("testDatehourNullVector", result->getItem(i)->getString(), values[i]->getString());
}

void testDateVector() {
	string beginDate = "2010.08.20";
	vector<int> testValues = { 1,10,100,1000,10000,100000 };
	vector<string> expectResults = { "2010.08.21","2010.08.30","2010.11.28","2013.05.16","2038.01.05","2284.06.04" };
	string script;
	for (int testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginDate + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < testValues.size(); i++) {
		ASSERTION("testDateVector", result->getString(i), expectResults[i]);
	}
}


void testDatenullVector(int vecSize) {
	vector<ConstantSP> values(vecSize, Util::createNullConstant(DT_DATE));
	string script;
	for (int i = 0;i < vecSize; i++)
		script += " " + values[i]->getString();
	ConstantSP result = conn.run(script);
	for (int i = 0;i < vecSize; i++)
		ASSERTION("testDateNullVector", result->getItem(i)->getString(), values[i]->getString());
}

void testDatetimeVector() {
	string beginDateTime = "2012.10.01 15:00:04";
	vector<int> testValues = { 1,100,1000,10000,100000,1000000,10000000 };
	vector<string> expectResults = { "2012.10.01T15:00:05","2012.10.01T15:01:44","2012.10.01T15:16:44","2012.10.01T17:46:44","2012.10.02T18:46:44","2012.10.13T04:46:44","2013.01.25T08:46:44" };
	string script;
	for (int testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginDateTime + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < testValues.size(); i++) {
		ASSERTION("testDatetimeVector", result->getString(i), expectResults[i]);
	}
}


void testTimeStampVector() {
	string beginTimeStamp = "2009.10.12T00:00:00.000";
	vector<long long> testValues = { 1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000 };
	vector<string> expectResults = { "2009.10.12T00:00:00.001","2009.10.12T00:00:00.010","2009.10.12T00:00:00.100",
		"2009.10.12T00:00:01.000","2009.10.12T00:00:10.000","2009.10.12T00:01:40.000","2009.10.12T00:16:40.000",
		"2009.10.12T02:46:40.000","2009.10.13T03:46:40.000","2009.10.23T13:46:40.000","2010.02.04T17:46:40.000",
		"2012.12.12T09:46:40.000","2041.06.20T01:46:40.000" };
	string script;
	for (long long testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginTimeStamp + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0; i < testValues.size(); i++) {
		ASSERTION("testTimeStampVector", result->getString(i), expectResults[i]);
	}
}


void testnanotimeVector() {
	string beginNanotime = "13:30:10.008007006";
	vector<long long> testValues = { 1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000 };
	vector<string> expectResults = { "13:30:10.008007007","13:30:10.008007016","13:30:10.008007106",
		"13:30:10.008008006","13:30:10.008017006","13:30:10.008107006","13:30:10.009007006",
		"13:30:10.018007006","13:30:10.108007006","13:30:11.008007006","13:30:20.008007006",
		"13:31:50.008007006","13:46:50.008007006" };
	string script;
	for (long long testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginNanotime + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0; i < testValues.size(); i++) {
		ASSERTION("testnanotimeVector", result->getString(i), expectResults[i]);
	}
}


void testnanotimestampVector() {
	string beginNanotimestamp = "2012.06.13T13:30:10.008007006";
	vector<long long> testValues = { 1,10,100,1000,10000,100000,1000000,10000000,100000000,1000000000,10000000000,100000000000,1000000000000,10000000000000,100000000000000,1000000000000000,10000000000000000,100000000000000000 };
	vector<string> expectResults = { "2012.06.13T13:30:10.008007007","2012.06.13T13:30:10.008007016","2012.06.13T13:30:10.008007106",
		"2012.06.13T13:30:10.008008006","2012.06.13T13:30:10.008017006","2012.06.13T13:30:10.008107006","2012.06.13T13:30:10.009007006",
		"2012.06.13T13:30:10.018007006","2012.06.13T13:30:10.108007006","2012.06.13T13:30:11.008007006","2012.06.13T13:30:20.008007006",
		"2012.06.13T13:31:50.008007006","2012.06.13T13:46:50.008007006","2012.06.13T16:16:50.008007006","2012.06.14T17:16:50.008007006","2012.06.25T03:16:50.008007006","2012.10.07T07:16:50.008007006","2015.08.14T23:16:50.008007006" };
	string script;
	for (long long testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginNanotimestamp + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0; i < testValues.size(); i++) {
		ASSERTION("testnanotimestampVector", result->getString(i), expectResults[i]);
	}
}


void testmonthVector() {
	string beginmonth = "2012.06M";
	vector<int> testValues = { 1,10,100,1000 };
	vector<string> expectResults = { "2012.07M","2013.04M","2020.10M","2095.10M" };
	string script;
	for (int testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = beginmonth + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0; i < testValues.size(); i++) {
		ASSERTION("testmonthVector", result->getString(i), expectResults[i]);
	}
}

void testtimeVector() {
	string begintime = "13:30:10.008";
	vector<int> testValues = { 1,10,100,1000,10000,100000,1000000,10000000 };
	vector<string> expectResults = { "13:30:10.009","13:30:10.018","13:30:10.108","13:30:11.008","13:30:20.008","13:31:50.008","13:46:50.008","16:16:50.008" };
	string script;
	for (int testValue : testValues) {
		script += " " + std::to_string(testValue);
	}
	script = begintime + " + " + script;
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0; i < testValues.size(); i++) {
		ASSERTION("testtimeVector", result->getString(i), expectResults[i]);
	}
}

void testSymbol() {
	vector<string> expectResults = { "XOM","y" };
	string script;
	script += "x=`XOM`y;y=symbol x;y;";
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < expectResults.size(); i++) {
		ASSERTION("testSymbol", result->getString(i), expectResults[i]);
	}
}


void testSymbolBase() {
	int64_t startTime, time;

	conn.run("v=symbol(string(1..2000000))");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run(getSymbolVector("v", 2000000));
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector optimize：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(string(1..2000000)) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run(getSymbolVector("v", 2000000));
	conn.run("t=table(v as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector optimize：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(string(1..2000000)) as sym,symbol(string(1..2000000)) as sym1,symbol(string(1..2000000)) as sym2,symbol(string(1..2000000)) as sym3,symbol(string(1..2000000)) as sym4,symbol(string(1..2000000)) as sym5,symbol(string(1..2000000)) as sym6,symbol(string(1..2000000)) as sym7,symbol(string(1..2000000)) as sym8,symbol(string(1..2000000)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=table(symbol(take(string(0..20000),2000000)) as sym,symbol(take(string(20000..40000),2000000)) as sym1,symbol(take(string(40000..60000),2000000)) as sym2,symbol(take(string(60000..80000),2000000)) as sym3,symbol(take(string(80000..100000),2000000)) as sym4,symbol(take(string(100000..120000),2000000)) as sym5,symbol(take(string(120000..140000),2000000)) as sym6,symbol(take(string(140000..160000),2000000)) as sym7,symbol(take(string(160000..180000),2000000)) as sym8,symbol(take(string(180000..200000),2000000)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors：" << time << "ms" << endl;

	//    conn.run("undef(all)");
	//    conn.run("m=symbol(string(1..2000000))$1000:2000");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("d=dict(symbol(string(1..2000000)),symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("s=set(symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set：" << time << "ms" << endl;

	conn.run("undef(all)");
	conn.run("t=(symbol(string(1..2000000)),symbol(string(1..2000000)))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple：" << time << "ms" << endl;

	conn.run("undef(all)");
}

void testSymbolSmall() {
	int64_t startTime, time;
	conn.run("v=symbol(string(1..200))");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run(getSymbolVector("v", 200));
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector optimize：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(string(1..200)) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run(getSymbolVector("v", 200));
	conn.run("t=table(v as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector optimize：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(string(1..200)) as sym,symbol(string(1..200)) as sym1,symbol(string(1..200)) as sym2,symbol(string(1..200)) as sym3,symbol(string(1..200)) as sym4,symbol(string(1..200)) as sym5,symbol(string(1..200)) as sym6,symbol(string(1..200)) as sym7,symbol(string(1..200)) as sym8,symbol(string(1..200)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(symbol(take(string(0..20000),200)) as sym,symbol(take(string(20000..40000),200)) as sym1,symbol(take(string(40000..60000),200)) as sym2,symbol(take(string(60000..80000),200)) as sym3,symbol(take(string(80000..100000),200)) as sym4,symbol(take(string(100000..120000),200)) as sym5,symbol(take(string(120000..140000),200)) as sym6,symbol(take(string(140000..160000),200)) as sym7,symbol(take(string(160000..180000),200)) as sym8,symbol(take(string(180000..200000),200)) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors：" << time << "ms" << endl;
	conn.run("undef(all)");

	//    conn.run("m =symbol(string(1..200))$10:20");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix：" << time << "ms" << endl;
	//    conn.run("undef(all)");

	conn.run("d=dict(symbol(string(1..200)),symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("s=set(symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=(symbol(string(1..200)),symbol(string(1..200)))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple：" << time << "ms" << endl;
	conn.run("undef(all)");
}

void testSymbolNull() {
	int64_t startTime, time;
	conn.run("v=take(symbol(`cy`fty``56e`f65dfyfv),2000000)");
	startTime = getTimeStampMs();
	conn.run("v");
	time = getTimeStampMs() - startTime;
	cout << "symbol vector：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with one symbol vector：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym1,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym2,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym3,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym4,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym5,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym6,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym7,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym8,take(symbol(`cy`fty``56e`f65dfyfv),2000000) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with same symbol vectors：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=table(take(symbol(`cdwy`fty``56e`f652dfyfv),2000000) as sym,take(symbol(`cy`f8ty``56e`f65dfyfv),2000000) as sym1,take(symbol(`c2587y`fty``56e`f65dfyfv),2000000) as sym2,take(symbol(`cy````f65dfy4fv),2000000) as sym3,take(symbol(`cy```56e`f65dfgyfv),2000000) as sym4,take(symbol(`cy`fty``56e`12547),2000000) as sym5,take(symbol(`cy`fty``e`f65d728fyfv),2000000) as sym6,take(symbol(`cy`fty``56e`),2000000) as sym7,take(symbol(`cy`fty``56e`111),2000000) as sym8,take(symbol(`c412y`ft575y```f65dfyfv),2000000) as sym9)");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "table with diff symbol vectors：" << time << "ms" << endl;
	conn.run("undef(all)");

	//    conn.run("m =take(symbol(`cy`fty```f65dfyfv),2000000)$1000:2000");
	//    startTime = getTimeStampMs();
	//    conn.run("m");
	//    time = getTimeStampMs()-startTime;
	//    cout << "symbol matrix：" << time << "ms" << endl;
	//    conn.run("undef(all)");

	conn.run("d=dict(take(symbol(`cy`fty``56e`f65dfyfv),2000000),take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("d");
	time = getTimeStampMs() - startTime;
	cout << "symbol dict：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("s=set(take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("s");
	time = getTimeStampMs() - startTime;
	cout << "symbol set：" << time << "ms" << endl;
	conn.run("undef(all)");

	conn.run("t=(take(symbol(`cy`fty``56e`f65dfyfv),2000000),take(symbol(`cy`fty``56e`f65dfyfv),2000000))");
	startTime = getTimeStampMs();
	conn.run("t");
	time = getTimeStampMs() - startTime;
	cout << "tuple symbol tuple：" << time << "ms" << endl;
	conn.run("undef(all)");
}

void testmixtimevectorUpload() {
	VectorSP dates = Util::createVector(DT_ANY, 5, 100);
	dates->set(0, Util::createMonth(2016, 6));
	dates->set(1, Util::createDate(2016, 5, 16));
	dates->set(2, Util::createDateTime(2016, 6, 6, 6, 12, 12));
	dates->set(3, Util::createNanoTime(6, 28, 36, 00));
	dates->set(4, Util::createNanoTimestamp(2020, 8, 20, 2, 20, 20, 00));
	vector<ConstantSP> mixtimedata = { dates };
	vector<string> mixtimename = { "Mixtime" };
	conn.upload(mixtimename, mixtimedata);
}

void testFunctionDef() {
	string script = "def funcAdd(a,b){return a + b};funcAdd(100,200);";
	ConstantSP result = conn.run(script);
	ASSERTION("testFunctionDef", result->getString(), string("300"));
}

void testMatrix() {
	vector<string> expectResults = { "{1,2}","{3,4}","{5,6}" };
	string script = "1..6$2:3";
	ConstantSP result = conn.run(script);
	for (unsigned int i = 0;i < expectResults.size(); i++) {
		ASSERTION("testMatrix", result->getString(i), expectResults[i]);
	}
}

void testTable() {
	string script;
	script += "n=20000\n";
	script += "syms=`IBM`C`MS`MSFT`JPM`ORCL`BIDU`SOHU`GE`EBAY`GOOG`FORD`GS`PEP`USO`GLD`GDX`EEM`FXI`SLV`SINA`BAC`AAPL`PALL`YHOO`KOH`TSLA`CS`CISO`SUN\n";
	script += "mytrades=table(09:30:00+rand(18000,n) as timestamp,rand(syms,n) as sym, 10*(1+rand(100,n)) as qty,5.0+rand(100.0,n) as price, 1..n as number,rand(syms,n) as sym_2);\n";
	script += "select min(number) as minNum, max(number) as maxNum from mytrades";

	ConstantSP table = conn.run(script);
	ASSERTION("testTable", table->getColumn(0)->getString(0), string("1"));
	ASSERTION("testTable", table->getColumn(1)->getString(0), string("20000"));
}


void testDictionary() {
	string script;
	script += "dict(1 2 3,symbol(`IBM`MSFT`GOOG))";
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
	for (int i = 0;i < 5;i++) {
		names->set(i, Util::createString("name_" + std::to_string(i)));
		dates->set(i, Util::createDate(2010, 1, i + 1));
		prices->set(i, Util::createDouble(i*i));
	}
	vector<string> allnames = { "names","dates","prices" };
	vector<ConstantSP> allcols = { names,dates,prices };
	conn.upload(allnames, allcols);//upload data to server
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
	cout << table->getString() << endl;
}

TableSP createDemoTable() {
	vector<string> colNames = { "name","date","price" };
	vector<DATA_TYPE> colTypes = { DT_STRING,DT_DATE,DT_DOUBLE };
	int colNum = 3, rowNum = 3;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0;i < colNum;i++)
		columnVecs.emplace_back(table->getColumn(i));

	for (int i = 0;i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString("name_" + std::to_string(i)));
		columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
		columnVecs[2]->set(i, Util::createDouble(i*i));
	}
	return table;
}

TableSP createDemoTableSetStringWrong() {
	vector<string> colNames = { "name","date","price" };
	vector<DATA_TYPE> colTypes = { DT_STRING,DT_DATE,DT_DOUBLE };
	int colNum = 3, rowNum = 3;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0;i < colNum;i++)
		columnVecs.emplace_back(table->getColumn(i));

	for (int i = 0;i < rowNum; i++) {
		//wrong usage
		columnVecs[0]->setString("name_" + std::to_string(i));
		columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
		columnVecs[2]->set(i, Util::createDouble(i*i));
	}
	return table;
}

TableSP createDemoTableSetString() {
	vector<string> colNames = { "name","date","price" };
	vector<DATA_TYPE> colTypes = { DT_STRING,DT_DATE,DT_DOUBLE };
	int colNum = 3, rowNum = 3;
	ConstantSP table = Util::createTable(colNames, colTypes, rowNum, 100);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0;i < colNum;i++)
		columnVecs.emplace_back(table->getColumn(i));

	for (int i = 0;i < rowNum; i++) {
		//wrong usage
		columnVecs[0]->setString(i, "name_" + std::to_string(i));
		columnVecs[1]->set(i, Util::createDate(2010, 1, i + 1));
		columnVecs[2]->set(i, Util::createDouble(i*i));
	}
	return table;
}

void testDiskTable() {
	TableSP table = createDemoTable();
	conn.upload("mt", table);
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
	cout << result->getString() << endl;

}

void testDFSTable() {
	string script;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
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
	cout << result->getString() << endl;
}


void testDimensionTable() {
	string script;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://db1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "dt = db.createTable(mt,`dt).append!(mt);";
	script += "select * from dt;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}


void ASSERTIONHASH(const string& test, const int ret[], const int expect[], int len) {
	bool equal = true;
	for (int i = 0;i<len;i++) {
		if (ret[i] != expect[i])
			equal = false;
	}

	if (!equal) {
		std::cout << "ASSERT FAIL--" << test << "-- expect return --";
		for (int i = 0;i<len;i++)
			std::cout << expect[i] << ",";
		cout << ", real return--";
		for (int i = 0;i<len;i++)
			std::cout << ret[i] << ",";
		cout << std::endl;
		fail++;
	}
	else
		pass++;

}

void testDFSTableSetStringWrong() {
	string script;
	TableSP table = createDemoTableSetStringWrong();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `demoTable;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "db.createPartitionedTable(mt,tableName,`date);";
	script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
	script += "select * from tradTable;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

void testDFSTableSetString() {
	string script;
	TableSP table = createDemoTableSetString();
	conn.upload("mt", table);
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://SAMPLE_TRDDB\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "tableName = `demoTable;";
	script += "db = database(dbPath,VALUE,2010.01.01..2010.01.30);";
	script += "db.createPartitionedTable(mt,tableName,`date);";
	script += "tradTable=database(dbPath).loadTable(tableName).append!(mt);";
	script += "select * from tradTable;";
	TableSP result = conn.run(script);
	cout << result->getString() << endl;
}

void testCharVectorHash() {
	vector<char> testValues{ 127,-127,12,0,-12,-128 };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 10,12,12,0,10,-1 },
	{ 41,18,12,0,4,-1 },
	{ 56,24,12,0,68,-1 },
	{ 30,5,12,0,23,-1 },
	{ 127,129,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createChar(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testCharVectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_CHAR, 0);
	v->appendChar(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testCharVectorHash", hv, expected[j], 6);
	}
}

void testShortVectorHash() {
	vector<short> testValues{ 32767,-32767,12,0,-12,-32768 };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 7,2,12,0,10,-1 },
	{ 1,15,12,0,4,-1 },
	{ 36,44,12,0,68,-1 },
	{ 78,54,12,0,23,-1 },
	{ 4088,265,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createShort(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_SHORT, 0);
	v->appendShort(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
	}
}
void testIntVectorHash() {
	vector<int> testValues{ INT_MAX,INT_MAX*(-1),12,0,-12,INT_MIN };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 10,12,12,0,10,-1 },
	{ 7,9,12,0,4,-1 },
	{ 39,41,12,0,68,-1 },
	{ 65,67,12,0,23,-1 },
	{ 127,129,12,0,244,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createInt(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testIntVectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_INT, 0);
	v->appendInt(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testIntVectorHash", hv, expected[j], 6);
	}
}

void testLongVectorHash() {
	vector<long long> testValues{ LLONG_MAX,(-1)*LLONG_MAX,12,0,-12,LLONG_MIN };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 7,9,12,0,4,-1 },
	{ 41,0,12,0,29,-1 },
	{ 4,6,12,0,69,-1 },
	{ 78,80,12,0,49,-1 },
	{ 4088,4090,12,0,4069,-1 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createLong(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testLongVectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_LONG, 0);
	v->appendLong(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testLongVectorHash", hv, expected[j], 6);
	}
}


void testStringVectorHash() {
	vector<string> testValues{ "9223372036854775807","helloworldabcdefghijklmnopqrstuvwxyz","智臾科技","hello,智臾科技","123abc您好！","" };
	int buckets[5] = { 13,43,71,97,4097 };
	int expected[5][6] = {
		{ 8,1,11,8,10,0 },
	{ 37,20,14,23,41,0 },
	{ 31,0,41,63,40,0 },
	{ 24,89,51,54,42,0 },
	{ 739,3737,814,3963,3488,0 } };
	int hv[6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (unsigned int i = 0;i < testValues.size(); i++) {
			ConstantSP val = Util::createString(testValues[i]);
			hv[i] = val->getHash(buckets[j]);
		}
		ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_STRING, 0);
	v->appendString(testValues.data(), testValues.size());
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testShortVectorHash", hv, expected[j], 6);
	}
}

void testUUIDvectorHash() {
	string script;
	script = "a=rand(uuid(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		ASSERTIONHASH("testUUIDvectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_UUID, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_UUID, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testUUIDvectorHash", hv, expected[j], 6);
	}
}

void testIpAddrvectorHash() {
	string script;
	script = "a=rand(ipaddr(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_IP, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		ASSERTIONHASH("testIpAddrvectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_IP, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_IP, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testIpAddrvectorHash", hv, expected[j], 6);
	}
}

void testInt128vectorHash() {
	string script;
	script = "a=rand(int128(),6);table(a as k,hashBucket(a,13) as v1,hashBucket(a,43) as v2,hashBucket(a,71) as v3,hashBucket(a,97) as v4,hashBucket(a,4097) as v5)";
	TableSP t = conn.run(script);

	int buckets[5] = { 13,43,71,97,4097 };
	int hv[6] = { 0 };
	int expected[5][6] = { 0 };

	for (unsigned int j = 0;j<5;j++) {
		for (int i = 0;i < t->size(); i++) {
			ConstantSP val = Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i));
			hv[i] = val->getHash(buckets[j]);
			expected[j][i] = t->getColumn(j + 1)->getInt(i);
		}
		ASSERTIONHASH("testInt128vectorHash", hv, expected[j], 6);
	}
	VectorSP v = Util::createVector(DT_INT128, 0);
	for (int i = 0;i < t->size(); i++)
		v->append(Util::parseConstant(DT_INT128, t->getColumn(0)->getString(i)));
	for (unsigned int j = 0;j<5;j++) {
		v->getHash(0, 6, buckets[j], hv);
		ASSERTIONHASH("testInt128vectorHash", hv, expected[j], 6);
	}
}

/*
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
*/

void testRun() {
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
	double array_x[] = { 1.5, 2.5, 7 };
	x->setDouble(0, 3, array_x);
	ConstantSP y = Util::createVector(DT_DOUBLE, 3);
	double array_y[] = { 8.5, 7.5, 3 };
	y->setDouble(0, 3, array_y);
	args.push_back(x);
	args.push_back(y);
	ConstantSP result = conn.run("add", args);
	cout << result->getString() << endl;
}

void tets_Block_Reader_DFStable() {
	string script;
	string script1;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
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
	BlockReaderSP reader = conn.run(script1, 4, 2, fetchsize1);
	ConstantSP t;
	int total = 0;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;
		ASSERTION("tets_Block_Reader_DFStable", t->size(), fetchsize1);
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("tets_Block_Reader_DFStable", total, 12453);

	int fetchsize2 = 8200;
	BlockReaderSP reader2 = conn.run(script1, 4, 2, fetchsize2);
	ConstantSP t2;
	int total2 = 0;
	int tmp = fetchsize2;
	while (reader2->hasNext()) {
		t2 = reader2->read();
		total2 += t2->size();
		//cout<< "read" <<t2->size()<<endl;
		ASSERTION("tets_Block_Reader_DFStable", t2->size(), tmp);
		tmp = 12453 - tmp;
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("tets_Block_Reader_DFStable", total2, 12453);

	int fetchsize3 = 15000;
	BlockReaderSP reader3 = conn.run(script1, 4, 2, fetchsize3);
	ConstantSP t3;
	int total3 = 0;
	while (reader3->hasNext()) {
		t3 = reader3->read();
		total3 += t3->size();
		//cout<< "read" <<t2->size()<<endl;
		ASSERTION("tets_Block_Reader_DFStable", t3->size(), 12453);
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("tets_Block_Reader_DFStable", total3, 12453);
}

void test_Block_Table() {
	string script;
	string script1;
	script += "rows=12453;";
	script += "testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);";
	//script += "select count(*) from testblock;";
	conn.run(script);
	script1 += "select * from testblock ";
	int fetchsize1 = 12453;
	BlockReaderSP reader = conn.run(script1, 4, 2, fetchsize1);
	ConstantSP t;
	int total = 0;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;
		ASSERTION("test_Block_Table", t->size(), fetchsize1);
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("test_Block_Table", total, 12453);

	int fetchsize2 = 8200;
	BlockReaderSP reader2 = conn.run(script1, 4, 2, fetchsize2);
	ConstantSP t2;
	int total2 = 0;
	int tmp = fetchsize2;
	while (reader2->hasNext()) {
		t2 = reader2->read();
		total2 += t2->size();
		//cout<< "read" <<t2->size()<<endl;
		ASSERTION("test_Block_Table", t2->size(), tmp);
		tmp = 12453 - tmp;
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("test_Block_Table", total2, 12453);

	int fetchsize3 = 15000;
	BlockReaderSP reader3 = conn.run(script1, 4, 2, fetchsize3);
	ConstantSP t3;
	int total3 = 0;
	while (reader3->hasNext()) {
		t3 = reader3->read();
		total3 += t3->size();
		//cout<< "read" <<t2->size()<<endl;
		ASSERTION("test_Block_Table", t3->size(), 12453);
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("test_Block_Table", total3, 12453);
}

void test_block_skipALL() {
	string script;
	script += "login(`admin,`123456);";
	script += R"(select * from loadTable("dfs://TEST_BLOCK","pt");)";
	BlockReaderSP reader = conn.run(script, 4, 2, 8200);
	ConstantSP t = reader->read();
	reader->skipAll();
	TableSP result = conn.run("table(1..100 as id1)");
	//cout<<result->getString()<<endl;
	ASSERTION("test_block_skipALL", result->size(), 100);

}

void test_huge_table() {
	string script;
	string script1;
	script += "rows=20000000;";
	script += "testblock=table(take(1,rows) as id,take(`A,rows) as symbol,take(2020.08.01..2020.10.01,rows) as date, rand(50,rows) as size,rand(50.5,rows) as price);";
	conn.run(script);
	script1 += "select * from testblock;";
	BlockReaderSP reader = conn.run(script1, 4, 2, 8200);
	ConstantSP t;
	int total = 0;
	int i = 1;
	int fetchsize = 8200;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;

		if (t->size() == 8200) {
			ASSERTION("test_huge_table", t->size(), 8200);
		}
		else {
			ASSERTION("test_huge_table", t->size(), 200);
		}

	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("test_huge_table", total, 20000000);
}


void test_huge_DFS() {
	string script;
	string script1;
	TableSP table = createDemoTable();
	conn.upload("mt", table);
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
	BlockReaderSP reader = conn.run(script1, 4, 2, fetchsize1);
	ConstantSP t;
	int total = 0;
	while (reader->hasNext()) {
		t = reader->read();
		total += t->size();
		//cout<< "read" <<t->size()<<endl;
		if (t->size() == 8200) {
			ASSERTION("test_huge_DFS", t->size(), 8200);
		}
		else {
			ASSERTION("test_huge_DFS", t->size(), 203);
		}

	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("test_huge_DFS", total, 20000003);

	int fetchsize2 = 2000000;
	BlockReaderSP reader2 = conn.run(script1, 4, 2, fetchsize2);
	ConstantSP t2;
	int total2 = 0;
	while (reader2->hasNext()) {
		t2 = reader2->read();
		total2 += t2->size();
		//cout<< "read" <<t2->size()<<endl;

		if (t2->size() == 2000000) {
			ASSERTION("test_huge_DFS", t2->size(), 2000000);
		}
		else {
			ASSERTION("test_huge_DFS", t2->size(), 3);
		}

		//ASSERTION("tets_Block_Reader_DFStable",t->size(),fetchsize1);
	}
	//cout<<"total is"<<total<<endl;
	ASSERTION("test_huge_DFS", total2, 20000003);

}

void test_PartitionedTableAppender_value_int() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,0..9);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_int", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_value_int", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_date1() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, 1, i % 10 + 1));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date1", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_date1", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(2012.01.01..2012.01.10, 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_date1", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_date2() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date2\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, DATETIME, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATETIME, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDateTime(2012, 1, i % 10 + 1, 9, 30, 30));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date2", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_date2", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(2012.01.01..2012.01.10, 1000000), 09:30:30) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_date2", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_date3() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date3\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, TIMESTAMP, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_TIMESTAMP, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createTimestamp(2012, 1, i % 10 + 1, 9, 30, 30, 125));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date3", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_date3", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(2012.01.01..2012.01.10, 1000000), 09:30:30.125) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_date3", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_date4() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_date4\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01.01..2012.01.10);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, NANOTIMESTAMP, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_NANOTIMESTAMP, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createNanoTimestamp(2012, 1, i % 10 + 1, 9, 30, 30, 00));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_date4", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_date4", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(2012.01.01..2012.01.10, 1000000), 09:30:30.000000000) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_date4", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_month1() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_month1\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01M..2012.10M);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, i % 10 + 1, 10));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_month1", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_month1", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_month1", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_month2() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_month2\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01M..2012.10M);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, DATETIME, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATETIME, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDateTime(2012, i % 10 + 1, 10, 9, 30, 30));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_month2", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_month2", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000), 09:30:30) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_month2", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_month3() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_month3\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,2012.01M..2012.10M);";
	script += "dummy = table(1:0, `id`sym`time`value, [INT, SYMBOL, TIMESTAMP, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`time);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "time", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_TIMESTAMP, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createTimestamp(2012, i % 10 + 1, 10, 9, 30, 30, 125));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_month3", "pt", "time", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_month3", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, concatDateTime(take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000), 09:30:30.125) as time, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by time, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by time, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_value_month3", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_string() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,\"A\"+string(0..9));";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_string", "pt", "sym", pool);
	appender.append(t1);
	// string script1;
	// script1 += "login(`admin,`123456);";
	// script1 += "exec count(*) from loadTable(dbPath, `pt)";
	// int total = 0;
	// total = conn.run(script1)->getInt();
	// ASSERTION("test_appender_value_string",total,1000000);
	// string script2;
	// script2 += "login(`admin,`123456);";
	// script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	// script2 += "expected = select *  from tmp order by id, sym, value;";
	// script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	// script2 += "each(eqObj, re.values(), expected.values()).int();";
	// ConstantSP result = conn.run(script2);
	// for(int i=0; i<3; i++)
	//     ASSERTION("test_appender_value_string", result->getInt(i), 1);
}

void test_PartitionedTableAppender_value_symbol() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,VALUE,\"A\"+string(0..9));";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_symbol", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_value_symbol", result->getInt(i), 1);
}

void test_PartitionedTableAppender_range_int() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "ranges = cutPoints(0..999999, 10);";
	script += "db = database(dbPath,RANGE,ranges);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_range_int", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_range_int", result->getInt(i), 1);
}

void test_PartitionedTableAppender_range_date() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_date\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,RANGE,date(2012.01M..2012.11M));";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, i % 10 + 1, 10));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_date", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_range_date", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_range_date", result->getInt(i), 1);
}

void test_PartitionedTableAppender_range_symbol() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "ranges = cutPoints(\"A\"+string(0..999999), 10);";
	script += "db = database(dbPath,RANGE,ranges);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_range_symbol", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_range_symbol", result->getInt(i), 1);
}

void test_PartitionedTableAppender_range_string() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_range_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "ranges = cutPoints(\"A\"+string(0..999999), 10);";
	script += "db = database(dbPath,RANGE,ranges);";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_range_string", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_range_string", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_range_string", result->getInt(i), 1);
}

void test_PartitionedTableAppender_hash_int() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[INT, 10]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_hash_int", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_hash_int", result->getInt(i), 1);
}

void test_PartitionedTableAppender_hash_date() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_date\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[DATE,10]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, i % 10 + 1, 10));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_date", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_hash_date", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(temporalAdd(2012.01.10, 0..9, \"M\"), 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_hash_date", result->getInt(i), 1);
}

void test_PartitionedTableAppender_hash_symbol() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[SYMBOL,10]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_hash_symbol", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_hash_symbol", result->getInt(i), 1);
}

void test_PartitionedTableAppender_hash_string() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_hash_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,HASH,[STRING,10]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_hash_string", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_hash_string", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(0..999999 as id, \"A\"+string(0..999999) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_hash_string", result->getInt(i), 1);
}

void test_PartitionedTableAppender_list_int() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_list_int\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[0..3, 4, 5..7, 8..9]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_list_int", "pt", "id", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_list_int", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_list_int", result->getInt(i), 1);
}

void test_PartitionedTableAppender_list_date() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_list_date\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[2012.01.01..2012.01.03, 2012.01.04..2012.01.07, 2012.01.08..2012.01.10]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, 1, i % 10 + 1));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_list_date", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_list_date1", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(2012.01.01..2012.01.10, 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_list_date1", result->getInt(i), 1);
}

void test_PartitionedTableAppender_list_string() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_list_string\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[`A0`A1, `A2, `A3, `A4`A5`A6`A7, `A8`A9]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, STRING, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_STRING, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_list_string", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_list_string", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_list_string", result->getInt(i), 1);
}

void test_PartitionedTableAppender_list_symbol() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://list_symbol\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db = database(dbPath,LIST,[`A0`A1, `A2, `A3, `A4`A5`A6`A7, `A8`A9]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://list_symbol", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_list_symbol", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_list_symbol", result->getInt(i), 1);
}

void test_PertitionedTableAppender_value_hash() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_hash\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",HASH,[INT, 2]);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_hash", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_hash", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_value_hash", result->getInt(i), 1);

}

void test_PartitionedTableAppender_value_range() {
	//create dfs db
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_value_range\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",RANGE,0 5 10);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_value_range", "pt", "sym", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_value_range", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..9), 1000000) as sym, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<3; i++)
		ASSERTION("test_appender_value_range", result->getInt(i), 1);
}

void test_PartitionedTableAppender_compo3() {
	string script;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_compo3\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,2012.01.01..2012.01.10);";
	script += "ranges=cutPoints(\"A\"+string(0..999), 5);";
	script += "db2 = database(\"\",RANGE,ranges);";
	script += "db3 = database(\"\",HASH,[INT, 2]);";
	script += "db = database(dbPath,COMPO,[db1, db2, db3]);";
	script += "dummy = table(1:0, `id`sym`date`value, [INT, SYMBOL, DATE, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`date`sym`id);";
	conn.run(script);
	vector<string> colNames = { "id", "sym", "date", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_DATE, DT_INT };
	int colNum = 4, rowNum = 1000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createDate(2012, 1, i % 10 + 1));
		columnVecs[3]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_compo3", "pt", "date", pool);
	appender.append(t1);
	string script1;
	script1 += "login(`admin,`123456);";
	script1 += "exec count(*) from loadTable(dbPath, `pt)";
	int total = 0;
	total = conn.run(script1)->getInt();
	ASSERTION("test_appender_compo3", total, 1000000);
	string script2;
	script2 += "login(`admin,`123456);";
	script2 += "tmp = table(take(0..9, 1000000) as id, take(\"A\"+string(0..999), 1000000) as sym, take(2012.01.01..2012.01.10, 1000000) as date, 0..999999 as value);";
	script2 += "expected = select *  from tmp order by date, id, sym, value;";
	script2 += "re = select * from loadTable(dbPath, `pt) order by date, id, sym, value;";
	script2 += "each(eqObj, re.values(), expected.values()).int();";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_appender_compo3", result->getInt(i), 1);
}

void test_symbol_optimization() {
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10" };
	vector<DATA_TYPE> colTypes = { DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL, DT_SYMBOL };
	int colNum = 10, rowNum = 2000000;
	TableSP t1 = Util::createTable(colNames, colTypes, rowNum, 2000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createString("A" + std::to_string(i % 1000)));
		columnVecs[1]->set(i, Util::createString("B" + std::to_string(i % 1000)));
		columnVecs[2]->set(i, Util::createString("C" + std::to_string(i % 1000)));
		columnVecs[3]->set(i, Util::createString("D" + std::to_string(i % 1000)));
		columnVecs[4]->set(i, Util::createString("E" + std::to_string(i % 1000)));
		columnVecs[5]->set(i, Util::createString("F" + std::to_string(i % 1000)));
		columnVecs[6]->set(i, Util::createString("G" + std::to_string(i % 1000)));
		columnVecs[7]->set(i, Util::createString("H" + std::to_string(i % 1000)));
		columnVecs[8]->set(i, Util::createString("I" + std::to_string(i % 1000)));
		columnVecs[9]->set(i, Util::createString("J" + std::to_string(i % 1000)));
	}
	int64_t startTime, time;
	startTime = getTimeStampMs();
	conn.upload("t1", { t1 });
	time = getTimeStampMs() - startTime;
	cout << "symbol table：" << time << "ms" << endl;
	string script1;
	script1 += "n=2000000;";
	script1 += "tmp=table(take(symbol(\"A\"+string(0..999)), n) as col1, take(symbol(\"B\"+string(0..999)), n) as col2, take(symbol(\"C\"+string(0..999)), n) as col3, take(symbol(\"D\"+string(0..999)), n) as col4, take(symbol(\"E\"+string(0..999)), n) as col5, take(symbol(\"F\"+string(0..999)), n) as col6, take(symbol(\"G\"+string(0..999)), n) as col7, take(symbol(\"H\"+string(0..999)), n) as col8, take(symbol(\"I\"+string(0..999)), n) as col9, take(symbol(\"J\"+string(0..999)), n) as col10);";
	script1 += "each(eqObj, t1.values(), tmp.values());";
	ConstantSP result = conn.run(script1);
	for (int i = 0; i<10; i++)
		ASSERTION("test_symbol_optimization", result->getInt(i), 1);
	ConstantSP res = conn.run("t1");
}

void test_AutoFitTableAppender_convert_to_date() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `date1`date2`date3`date4, [DATE, DATE, DATE, DATE]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "date1", "date2", "date3", "date4" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 4, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, i + 1, 12, 30, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, i + 1, 12, 30, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, i + 1, 12, 30, 30, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, 1, i + 1));
	}
	int res = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_date1", res, rowNum);
	string script2;
	script2 += "expected = table(1969.01.01+0..9 as date1, 1969.01.01+0..9 as date2, 1969.01.01+0..9 as date3, 1969.01.01+0..9 as date4);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<4; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_date2", result->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, i + 1, 12, 30, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, i + 1, 12, 30, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, i + 1, 12, 30, 30, 123456789));
		columnVecs1[3]->set(i, Util::createDate(2012, 1, i + 1));
	}
	int res2 = appender.append(tmp2);
	ASSERTION("test_AutoFitTableAppender_convert_to_date1", res2, rowNum);
	string script3;
	script3 += "expected = table(2012.01.01+0..9 as date1, 2012.01.01+0..9 as date2, 2012.01.01+0..9 as date3, 2012.01.01+0..9 as date4);";
	script3 += "each(eqObj, (select * from st1 where date1>1970.01.01).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<4; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_date2", result2->getInt(i), 1);

}

void test_AutoFitTableAppender_convert_to_month() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `month1`month2`month3`month4`month5, [MONTH, MONTH, MONTH, MONTH, MONTH]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "month1", "month2", "month3", "month4", "month5" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE, DT_MONTH };
	int colNum = 5, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, i + 1, 1, 12, 30, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, i + 1, 1, 12, 30, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, i + 1, 1, 12, 30, 30, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, i + 1, 1));
		columnVecs[4]->set(i, Util::createMonth(1969, i + 1));
	}
	int res1 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_month1", res1, rowNum);
	string script2;
	script2 += "expected = table(1969.01M+0..9 as month1, 1969.01M+0..9 as month2, 1969.01M+0..9 as month3, 1969.01M+0..9 as month4, 1969.01M+0..9 as month5);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<5; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_month2", result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs2;
	columnVecs2.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs2.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs2[0]->set(i, Util::createDateTime(2000, i + 1, 1, 12, 30, 30));
		columnVecs2[1]->set(i, Util::createTimestamp(2000, i + 1, 1, 12, 30, 30, 123));
		columnVecs2[2]->set(i, Util::createNanoTimestamp(2000, i + 1, 1, 12, 30, 30, 123456789));
		columnVecs2[3]->set(i, Util::createDate(2000, i + 1, 1));
		columnVecs2[4]->set(i, Util::createMonth(2000, i + 1));
	}
	int res2 = appender.append(tmp2);
	ASSERTION("test_AutoFitTableAppender_convert_to_month3", res2, rowNum);
	string script3;
	script3 += "expected = table(2000.01M+0..9 as month1, 2000.01M+0..9 as month2, 2000.01M+0..9 as month3, 2000.01M+0..9 as month4, 2000.01M+0..9 as month5);";
	script3 += "each(eqObj,(select * from st1 where month1>1970.01M).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<5; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_month4", result2->getInt(i), 1);

}

void test_AutoFitTableAppender_convert_to_datetime() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4, [DATETIME, DATETIME, DATETIME, DATETIME]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 4, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, 30, i));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, 30, i, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, 30, i, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, 1, 1));
	}
	int res1 = appender.append(tmp);
	ASSERTION("test_AutoFitTableAppender_convert_to_datetime1", res1, rowNum);
	string script2;
	script2 += "expected = table(1969.01.01T12:30:00+0..9 as col1, 1969.01.01T12:30:00+0..9 as col2, 1969.01.01T12:30:00+0..9 as col3, take(1969.01.01T00:00:00, 10) as col4);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_datetime2", result1->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2001, 1, 1, 12, 30, i));
		columnVecs1[1]->set(i, Util::createTimestamp(2001, 1, 1, 12, 30, i, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2001, 1, 1, 12, 30, i, 123456789));
		columnVecs1[3]->set(i, Util::createDate(2001, 1, 1));
	}
	int res2 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_datetime3", res2, rowNum);
	string script3;
	script3 += "expected = table(2001.01.01T12:30:00+0..9 as col1, 2001.01.01T12:30:00+0..9 as col2, 2001.01.01T12:30:00+0..9 as col3, take(2001.01.01T00:00:00, 10) as col4);";
	script3 += "each(eqObj, (select * from st1 where col1>1970.01.01T00:00:00).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_datetime4", result2->getInt(i), 1);
}

void test_AutoFitTableAppender_convert_to_timestamp() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5, [TIMESTAMP, TIMESTAMP, TIMESTAMP, TIMESTAMP,TIMESTAMP]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 5, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, 30, i));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, 30, i, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, 30, i, 123456789));
		columnVecs[3]->set(i, Util::createNanoTimestamp(1960, 1, 1, 12, 30, 10, 8000000));
		columnVecs[4]->set(i, Util::createDate(1969, 1, 1));
	}
	int res = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_timestamp1", res, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(1969.01.01T12:30:00.000, 0..9, \"s\") as col1, temporalAdd(1969.01.01T12:30:00.123, 0..9, \"s\") as col2, temporalAdd(1969.01.01T12:30:00.123, 0..9, \"s\") as col3, take(1960.01.01T12:30:10.008, 10) as col4, take(1969.01.01T00:00:00.000, 10) as col5);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_timestamp2", result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2000, 1, 1, 12, 30, i));
		columnVecs1[1]->set(i, Util::createTimestamp(2000, 1, 1, 12, 30, i, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2000, 1, 1, 12, 30, i, 123456789));
		columnVecs1[3]->set(i, Util::createNanoTimestamp(2000, 1, 1, 12, 30, 10, 8000000));
		columnVecs1[4]->set(i, Util::createDate(2000, 1, 1));
	}
	int res2 = appender.append(tmp2);
	ASSERTION("test_AutoFitTableAppender_convert_to_timestamp3", res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(2000.01.01T12:30:00.000, 0..9, \"s\") as col1, temporalAdd(2000.01.01T12:30:00.123, 0..9, \"s\") as col2, temporalAdd(2000.01.01T12:30:00.123, 0..9, \"s\") as col3, take(2000.01.01T12:30:10.008, 10) as col4, take(2000.01.01T00:00:00.000, 10) as col5);";
	script3 += "each(eqObj, (select * from st1 where date(col1)>1970.01.01).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_timestamp4", result2->getInt(i), 1);

}

void test_AutoFitTableAppender_convert_to_nanotimestamp() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4, [NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP, NANOTIMESTAMP]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_DATE };
	int colNum = 4, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, 30, i));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, 30, i, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, 30, i, 123456789));
		columnVecs[3]->set(i, Util::createDate(1969, 1, 1));
	}
	int res1 = appender.append(tmp);
	ASSERTION("test_AutoFitTableAppender_convert_to_nanotimestamp1", res1, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(1969.01.01T12:30:00.000000000, 0..9, \"s\") as col1, temporalAdd(1969.01.01T12:30:00.123000000, 0..9, \"s\") as col2, temporalAdd(1969.01.01T12:30:00.123456789, 0..9, \"s\") as col3, take(1969.01.01T00:00:00.000000000, 10) as col4);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_nanotimestamp2", result->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, 30, i));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, 30, i, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, 30, i, 123456789));
		columnVecs1[3]->set(i, Util::createDate(2012, 1, 1));
	}
	int res2 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_nanotimestamp3", res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(2012.01.01T12:30:00.000000000, 0..9, \"s\") as col1, temporalAdd(2012.01.01T12:30:00.123000000, 0..9, \"s\") as col2, temporalAdd(2012.01.01T12:30:00.123456789, 0..9, \"s\") as col3, take(2012.01.01T00:00:00.000000000, 10) as col4);";
	script3 += "each(eqObj, (select * from st1 where col1>=2012.01.01T12:30:00.000000000).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_nanotimestamp4", result2->getInt(i), 1);
}

void test_AutoFitTableAppender_convert_to_minute() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [MINUTE, MINUTE, MINUTE, MINUTE, MINUTE, MINUTE, MINUTE]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res1 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_minute1", res1, rowNum);
	string script2;
	script2 += "expected = table(12:00m+0..9 as col1, 12:00m+0..9 as col2, 12:00m+0..9 as col3, 12:00m+0..9 as col4, 12:00m+0..9 as col5, 12:00m+0..9 as col6, 12:00m+0..9 as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_minute2", result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2000, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2000, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2000, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp2);
	ASSERTION("test_AutoFitTableAppender_convert_to_minute3", res2, rowNum);
	string script3;
	script3 += "expected = table(12:10m+0..9 as col1, 12:10m+0..9 as col2, 12:10m+0..9 as col3, 12:10m+0..9 as col4, 12:10m+0..9 as col5, 12:10m+0..9 as col6, 12:10m+0..9 as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10m).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_minute4", result2->getInt(i), 1);
}

void test_AutoFitTableAppender_convert_to_time() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [TIME, TIME, TIME, TIME, TIME, TIME, TIME]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res1 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_time1", res1, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(12:00:30.000, 0..9, \"m\") as col1, temporalAdd(12:00:30.123, 0..9, \"m\") as col2, temporalAdd(12:00:30.123, 0..9, \"m\") as col3, temporalAdd(12:00:30.123, 0..9, \"m\") as col4, temporalAdd(12:00:30.000, 0..9, \"m\") as col5, temporalAdd(12:00:00.000, 0..9, \"m\") as col6, temporalAdd(12:00:30.123, 0..9, \"m\") as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_time2", result1->getInt(i), 1);

	TableSP tmp2 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp2->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp2);
	ASSERTION("test_AutoFitTableAppender_convert_to_time3", res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(12:10:30.000, 0..9, \"m\") as col1, temporalAdd(12:10:30.123, 0..9, \"m\") as col2, temporalAdd(12:10:30.123, 0..9, \"m\") as col3, temporalAdd(12:10:30.123, 0..9, \"m\") as col4, temporalAdd(12:10:30.000, 0..9, \"m\") as col5, temporalAdd(12:10:00.000, 0..9, \"m\") as col6, temporalAdd(12:10:30.123, 0..9, \"m\") as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10:30.000).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_time4", result2->getInt(i), 1);

}

void test_AutoFitTableAppender_convert_to_second() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [SECOND, SECOND, SECOND, SECOND, SECOND, SECOND, SECOND]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res = appender.append(tmp);
	ASSERTION("test_AutoFitTableAppender_convert_to_second1", res, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(12:00:30, 0..9, \"m\") as col1, temporalAdd(12:00:30, 0..9, \"m\") as col2, temporalAdd(12:00:30, 0..9, \"m\") as col3, temporalAdd(12:00:30, 0..9, \"m\") as col4, temporalAdd(12:00:30, 0..9, \"m\") as col5, temporalAdd(12:00:00, 0..9, \"m\") as col6, temporalAdd(12:00:30, 0..9, \"m\") as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_second2", result->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_second3", res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(12:10:30, 0..9, \"m\") as col1, temporalAdd(12:10:30, 0..9, \"m\") as col2, temporalAdd(12:10:30, 0..9, \"m\") as col3, temporalAdd(12:10:30, 0..9, \"m\") as col4, temporalAdd(12:10:30, 0..9, \"m\") as col5, temporalAdd(12:10:00, 0..9, \"m\") as col6, temporalAdd(12:10:30, 0..9, \"m\") as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10:30).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_second4", result2->getInt(i), 1);

}

void test_AutoFitTableAppender_convert_to_nanotime() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5`col6`col7, [NANOTIME, NANOTIME, NANOTIME, NANOTIME, NANOTIME, NANOTIME, NANOTIME]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4", "col5", "col6", "col7" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME, DT_TIMESTAMP, DT_NANOTIMESTAMP, DT_TIME, DT_SECOND, DT_MINUTE, DT_NANOTIME };
	int colNum = 7, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(1969, 1, 1, 12, i, 30));
		columnVecs[1]->set(i, Util::createTimestamp(1969, 1, 1, 12, i, 30, 123));
		columnVecs[2]->set(i, Util::createNanoTimestamp(1969, 1, 1, 12, i, 30, 123456789));
		columnVecs[3]->set(i, Util::createTime(12, i, 30, 123));
		columnVecs[4]->set(i, Util::createSecond(12, i, 30));
		columnVecs[5]->set(i, Util::createMinute(12, i));
		columnVecs[6]->set(i, Util::createNanoTime(12, i, 30, 123456789));
	}
	int res = appender.append(tmp);
	ASSERTION("test_AutoFitTableAppender_convert_to_nanotime1", res, rowNum);
	string script2;
	script2 += "expected = table(temporalAdd(12:00:30.000000000, 0..9, \"m\") as col1, temporalAdd(12:00:30.123000000, 0..9, \"m\") as col2, temporalAdd(12:00:30.123456789, 0..9, \"m\") as col3, temporalAdd(12:00:30.123000000, 0..9, \"m\") as col4, temporalAdd(12:00:30.000000000, 0..9, \"m\") as col5, temporalAdd(12:00:00.000000000, 0..9, \"m\") as col6, temporalAdd(12:00:30.123456789, 0..9, \"m\") as col7);";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result = conn.run(script2);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_nanotime2", result->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDateTime(2012, 1, 1, 12, i + 10, 30));
		columnVecs1[1]->set(i, Util::createTimestamp(2012, 1, 1, 12, i + 10, 30, 123));
		columnVecs1[2]->set(i, Util::createNanoTimestamp(2012, 1, 1, 12, i + 10, 30, 123456789));
		columnVecs1[3]->set(i, Util::createTime(12, i + 10, 30, 123));
		columnVecs1[4]->set(i, Util::createSecond(12, i + 10, 30));
		columnVecs1[5]->set(i, Util::createMinute(12, i + 10));
		columnVecs1[6]->set(i, Util::createNanoTime(12, i + 10, 30, 123456789));
	}
	int res2 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_nanotime3", res2, rowNum);
	string script3;
	script3 += "expected = table(temporalAdd(12:10:30.000000000, 0..9, \"m\") as col1, temporalAdd(12:10:30.123000000, 0..9, \"m\") as col2, temporalAdd(12:10:30.123456789, 0..9, \"m\") as col3, temporalAdd(12:10:30.123000000, 0..9, \"m\") as col4, temporalAdd(12:10:30.000000000, 0..9, \"m\") as col5, temporalAdd(12:10:00.000000000, 0..9, \"m\") as col6, temporalAdd(12:10:30.123456789, 0..9, \"m\") as col7);";
	script3 += "each(eqObj, (select * from st1 where col1>=12:10:30.000000000).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_nanotime4", result2->getInt(i), 1);
}

void test_AutoFitTableAppender_dfs_table() {
	//convert datetime to date
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "dbPath='dfs://test_autoTableFitAppender';";
	script1 += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script1 += "db=database(dbPath, VALUE, 2012.01.01..2012.01.10);";
	script1 += "t=table(1:0, [`date], [DATE]);";
	script1 += "pt=db.createPartitionedTable(t, `pt, `date);";
	conn.run(script1);
	AutoFitTableAppender appender("dfs://test_autoTableFitAppender", "pt", conn);
	vector<string> colNames = { "date" };
	vector<DATA_TYPE> colTypes = { DT_DATETIME };
	int colNum = 1, rowNum = 10;
	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDateTime(2012, 1, i + 1, 12, 30, 30));
	}
	int res = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_dfs_datetime_to_date1", res, rowNum);
	string script;
	script += "expected = table(temporalAdd(2012.01.01, 0..9, 'd') as date);";
	script += "each(eqObj, (select * from pt order by date).values(), (select * from expected order by date).values());";
	ConstantSP result = conn.run(script);
	ASSERTION("test_AutoFitTableAppender_dfs_datetime_to_date2", result->getInt(0), 1);
}

void test_AutoFitTableAppender_convert_to_datehour() {
	string script1;
	script1 += "login('admin', '123456');";
	script1 += "try{undef(`st1);undef(`st1, SHARED)}catch(ex){print ex};";
	script1 += "share table(100:0, `col1`col2`col3`col4`col5, [DATEHOUR, DATEHOUR , DATEHOUR , DATEHOUR, DATEHOUR]) as st1;";
	conn.run(script1);
	AutoFitTableAppender appender("", "st1", conn);
	vector<string> colNames = { "col1", "col2", "col3", "col4","col5" };
	vector<DATA_TYPE> colTypes = { DT_DATE,DT_DATEHOUR, DT_DATETIME,DT_TIMESTAMP, DT_NANOTIMESTAMP, };
	int colNum = 5, rowNum = 10;
	TableSP tmp = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(tmp->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createDate(1969, 1, 1));
		columnVecs[1]->set(i, Util::createDateHour(1969, 1, 1, i + 1));
		columnVecs[2]->set(i, Util::createDateTime(1969, 1, 1, i + 1, 30, 30));
		columnVecs[3]->set(i, Util::createTimestamp(1969, 1, 1, i + 1, 30, 30, 123));
		columnVecs[4]->set(i, Util::createNanoTimestamp(1969, 1, 1, i + 1, 30, 30, 123456789));

	}
	int res1 = appender.append(tmp);
	ASSERTION("test_AutoFitTableAppender_convert_to_datehour1", res1, rowNum);
	string script2;
	script2 += "expected =table(take(datehour(1969.01.01T00:00:00),10) as col1, datehour(1969.01.01T01:00:00)+0..9 as col2, datehour(1969.01.01T01:00:00)+0..9 as col3, datehour(1969.01.01T01:00:00)+0..9 as col4, datehour(1969.01.01T01:00:00)+0..9 as col5  );";
	script2 += "each(eqObj, st1.values(), expected.values());";
	ConstantSP result1 = conn.run(script2);


	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_datehour2", result1->getInt(i), 1);

	TableSP tmp1 = Util::createTable(colNames, colTypes, rowNum, rowNum);
	vector<VectorSP> columnVecs1;
	columnVecs1.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs1.emplace_back(tmp1->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs1[0]->set(i, Util::createDate(2001, 1, 1));
		columnVecs1[1]->set(i, Util::createDateHour(2001, 1, 1, i + 1));
		columnVecs1[2]->set(i, Util::createDateTime(2001, 1, 1, i + 1, 30, 30));
		columnVecs1[3]->set(i, Util::createTimestamp(2001, 1, 1, i + 1, 30, 30, 123));
		columnVecs1[4]->set(i, Util::createNanoTimestamp(2001, 1, 1, i + 1, 30, 30, 123456789));
	}
	int res2 = appender.append(tmp1);
	ASSERTION("test_AutoFitTableAppender_convert_to_datehour3", res2, rowNum);
	string script3;

	script3 += "expected =table(take(datehour(2001.01.01T00:00:00),10) as col1, datehour(2001.01.01T01:00:00)+0..9 as col2, datehour(2001.01.01T01:00:00)+0..9 as col3, datehour(2001.01.01T01:00:00)+0..9 as col4, datehour(2001.01.01T01:00:00)+0..9 as col5  );";
	script3 += "each(eqObj, (select * from st1 where col1>datehour(1970.01.01T00:00:00)).values(), expected.values());";
	ConstantSP result2 = conn.run(script3);
	for (int i = 0; i<colNum; i++)
		ASSERTION("test_AutoFitTableAppender_convert_to_datehour4", result2->getInt(i), 1);
}


void testClearMemory_var() {
	string script, script1;
	script += "login('admin', '123456');";
	script += "testVar=1000000";
	conn.run(script, 4, 2, 0, true);
	string result = conn.run("objs()[`name][0]")->getString();

	ASSERTION("testClearMemory_var", result.length(), size_t(0));
}

void testClearMemory_() {
	string script, script1, script2;
	script += "login('admin', '123456');";
	script += "dbPath='dfs://testClearMemory_';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath, VALUE, 2012.01.01..2012.01.10);";
	script += "t=table(1:0, [`date], [DATE]);";
	script += "pt=db.createPartitionedTable(t, `pt, `date);";
	script += "getSessionMemoryStat()[`memSize][0];";
	script += "select * from pt;";
	conn.run(script, 4, 2, 0, true);
	string result = conn.run("objs()[`name][0]")->getString();
	ASSERTION("testClearMemory_", result.length(), size_t(0));
}

void test_mutithread_basic() {
	int64_t startTime, time;
	startTime = getTimeStampMs();
	pool.run("sleep(1000)", 0);
	pool.run("sleep(3000)", 1);
	pool.run("sleep(5000)", 2);
	while (true) {
		if (pool.isFinished(0) && pool.isFinished(1) && pool.isFinished(2)) {
			time = (getTimeStampMs() - startTime) / 1000;
			ASSERTION("test_mutithread_basic", (int)time, 5);
			break;
		}
	}
}
void test_mutithread_WandR() {
	int64_t startTime, time;
	startTime = getTimeStampMs();
	string script, script1;
	script += "login(`admin,`123456);";
	script += "dbPath = \"dfs://test_mutithread_\";";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script += "db2 = database(\"\",RANGE,0 5 10);";
	script += "db = database(dbPath, COMPO, [db1, db2]);";
	script += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	pool.run(script, 0);
	//create tableSP
	vector<string> colNames = { "id", "sym", "value" };
	vector<DATA_TYPE> colTypes = { DT_INT, DT_SYMBOL, DT_INT };
	int colNum = 3, rowNum = 1000000;
	TableSP t = Util::createTable(colNames, colTypes, rowNum, 1000000);
	vector<VectorSP> columnVecs;
	columnVecs.reserve(colNum);
	for (int i = 0; i < colNum; i++)
		columnVecs.emplace_back(t->getColumn(i));
	for (int i = 0; i < rowNum; i++) {
		columnVecs[0]->set(i, Util::createInt(i % 10));
		columnVecs[1]->set(i, Util::createString("A" + std::to_string(i % 10)));
		columnVecs[2]->set(i, Util::createInt(i));
	}
	PartitionedTableAppender appender("dfs://test_mutithread_", "pt", "sym", pool);
	appender.append(t);

	script1 += "login(`admin,`123456);";
	script1 += "dbPath = \"dfs://test_mutithread_1\";";
	script1 += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script1 += "db1 = database(\"\",VALUE,`A0`A1`A2`A3`A4`A5`A6`A7`A8`A9);";
	script1 += "db2 = database(\"\",RANGE,0 5 10);";
	script1 += "db = database(dbPath, COMPO, [db1, db2]);";
	script1 += "dummy = table(1:0, `id`sym`value, [INT, SYMBOL, INT]);";
	script1 += "pt = db.createPartitionedTable(dummy,`pt,`sym`id);";
	conn.run(script1);
	PartitionedTableAppender appender1("dfs://test_mutithread_1", "pt", "sym", pool);
	appender1.append(t);
	time = getTimeStampMs() - startTime;
	cout << "test_mutithread_：" << time << "ms" << endl;

}

void test_mutithread_Session() {
	string script, script1;
	script += "getSessionMemoryStat().size()";
	int count1 = conn.run(script)->getInt();
	pool.shutDown();
	int count2 = conn.run(script)->getInt();
	ASSERTION("test_mutithread_Session", count1 - count2, 10);
}

void test_batchTableWriter_insert() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,2010.01.01..2010.01.10);";
	script += "t=table(100:0,`dbbool `dbchar `dbshort `dbstring_char `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour , [BOOL, CHAR, SHORT, STRING, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR]);";
	script += "share table(100:0,`dbbool `dbchar `dbshort `dbstring_char `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour , [BOOL, CHAR, SHORT, STRING, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR]) as table1;";
	script += "db.createTable(t,`dtable).append!(t);";
	script += "db.createPartitionedTable(t,`ptable,`dbdate).append!(t);";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("table1", "");
	const char* strtest = "test";
	long long testlong = 100;
	string expected = "expected = table( bool(1) as dbbool, char('t') as dbchar, short(100) as dbshort, string('test') as dbstring_char, string('test') as dbstring, long(100) as dblong, nanotime(100) as dbnanotime, nanotimestamp(100) as dbnanotimestamp, timestamp(100) as dbtimestamp,float(100) as dbfloat, double(100) as dbdouble, int(100) as dbint, date(100) as dbdate, month(100) as dbmonth, time(100) as dbtime, second(100) as dbsecond, minute(100) as dbminute, datetime(100) as dbdatetime, datehour(100) as dbdatehour);";
	conn.run(expected);
	string expected1 = "expected1 = table( take(bool(1),3003000) as dbbool, take(char('t'),3003000) as dbchar, take(short(100),3003000) as dbshort, take(string('test'),3003000) as dbstring_char, take(string('test'),3003000) as dbstring, take(long(100),3003000) as dblong, take(nanotime(100),3003000) as dbnanotime, take(nanotimestamp(100),3003000) as dbnanotimestamp, take(timestamp(100),3003000) as dbtimestamp, take(float(100),3003000) as dbfloat, take(double(100),3003000) as dbdouble, take(int(100),3003000) as dbint, take(date(100),3003000) as dbdate, take(month(100),3003000) as dbmonth, take(time(100),3003000) as dbtime, take(second(100),3003000) as dbsecond, take(minute(100),3003000) as dbminute, take(datetime(100),3003000) as dbdatetime, take(datehour(100),3003000) as dbdatehour);";
	conn.run(expected1);
	try {
		btw.insert("table1", "", int(0), int(0), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
		cout << "FAIL--test_batchTableWriter_insert1" << endl;
	}
	catch (std::exception& e) {
		cout << "PASSED--test_batchTableWriter_insert1" << endl;
	}

	try {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
		cout << "FAIL--test_batchTableWriter_insert2" << endl;
	}
	catch (std::exception& e) {
		cout << "PASSED--test_batchTableWriter_insert2" << endl;
	}
	//size1000
	for (int i = 0;i<1000;i++) {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(1000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	int result1 = 0;
	for (int i = 0;i<1000;i++) {
		string script = "eqObj(table1[" + std::to_string(i) + "].values(),expected[0].values());";
		result1 += conn.run(script)->getInt();
	}
	ASSERTION("test_batchTableWriter_insert3", result1, 1000);
	//size2000
	for (int i = 0;i<2000;i++) {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	result1 = 0;
	for (int i = 0;i<3000;i++) {
		string script = "eqObj(table1[" + std::to_string(i) + "].values(),expected[0].values());";
		result1 += conn.run(script)->getInt();
	}
	ASSERTION("test_batchTableWriter_insert4", result1, 3000);
	//size3000000
	for (int i = 0;i<3000000;i++) {
		btw.insert("table1", "", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 60;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3003000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	script = "eqObj(table1.values(),expected1.values());";
	result1 = conn.run(script)->getInt();
	ASSERTION("test_batchTableWriter_insert5", result1, 1);
	btw.removeTable("table1", "");

	//size1000
	btw.addTable("dfs://test_batchTableWriter", "dtable");
	for (int i = 0;i<1000;i++) {
		btw.insert("dfs://test_batchTableWriter", "dtable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(1000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	int result2 = 0;
	for (int i = 0;i<1000;i++) {
		string script = "dt=loadTable('dfs://test_batchTableWriter', 'dtable'); eqObj(( select * from dt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result2 += conn.run(script)->getInt();
	}
	ASSERTION("test_batchTableWriter_insert6", result2, 1000);
	//size2000
	for (int i = 0;i<2000;i++) {
		btw.insert("dfs://test_batchTableWriter", "dtable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	result2 = 0;
	for (int i = 0;i<3000;i++) {
		string script = "dt=loadTable('dfs://test_batchTableWriter', 'dtable'); eqObj(( select * from dt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result2 += conn.run(script)->getInt();
	}
	ASSERTION("test_batchTableWriter_insert7", result2, 3000);
	//size3000000
	for (int i = 0;i<3000000;i++) {
		btw.insert("dfs://test_batchTableWriter", "dtable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 60;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3003000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	result2 = 0;
	script = "eqObj(( select * from dt).values(),expected1.values());";
	result2 = conn.run(script)->getInt();
	ASSERTION("test_batchTableWriter_insert8", result2, 1);
	btw.removeTable("dfs://test_batchTableWriter", "dtable");

	//size1000
	btw.addTable("dfs://test_batchTableWriter", "ptable");
	for (int i = 0;i<1000;i++) {
		btw.insert("dfs://test_batchTableWriter", "ptable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(1000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	int result3 = 0;
	for (int i = 0;i<1000;i++) {
		string script = "pt=loadTable('dfs://test_batchTableWriter', 'ptable'); eqObj(( select * from pt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result3 += conn.run(script)->getInt();
	}
	ASSERTION("test_batchTableWriter_insert9", result3, 1000);
	//size2000
	for (int i = 0;i<2000;i++) {
		btw.insert("dfs://test_batchTableWriter", "ptable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	result3 = 0;
	for (int i = 0;i<3000;i++) {
		string script = "pt=loadTable('dfs://test_batchTableWriter', 'ptable'); eqObj(( select * from pt)[" + std::to_string(i) + "].values(),expected[0].values());";
		result3 += conn.run(script)->getInt();
	}
	ASSERTION("test_batchTableWriter_insert10", result3, 3000);
	//size3000000
	for (int i = 0;i<3000000;i++) {
		btw.insert("dfs://test_batchTableWriter", "ptable", char('0'), char('t'), short(100), strtest, string("test"), testlong, testlong, testlong, testlong, float(100), double(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100), int(100));
	}
	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3003000)) break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}

	for (int j = 0;j < 20;j++) {
		TableSP result = btw.getAllStatus();
		if (result->getColumn(3)->getString(0).c_str() == to_string(3003000)) { 
			break; 
		} else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}

	result3 = 0;
	script = "eqObj(( select * from pt).values(),expected1.values());";
	result3 = conn.run(script)->getInt();
	ASSERTION("test_batchTableWriter_insert11", result3, 1);
	btw.removeTable("dfs://test_batchTableWriter", "ptable");
}

void test_batchTableWriter_addTable() {


	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as testadd;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);

	try {
		btw.addTable("testadd", "");
		btw.addTable("testadd", "");
		cout << "FAIL--test_batchTableWriter_addTable1" << endl;

	}
	catch (std::exception& e) {
		cout << "PASSED--test_batchTableWriter_addTable1" << endl;
	}

	btw.removeTable("testadd", "");
}


void test_batchTableWriter_removeTable() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as testremove;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("testremove", "");

	try {
		btw.removeTable("test_remove", "");
		cout << "PASSED--test_batchTableWriter_removeTable1" << endl;
	}
	catch (std::exception& e) {
		cout << "FAIL--test_batchTableWriter_removeTable1" << endl;
	}
}

void test_batchTableWriter_insert_thread_fuction(int id, BatchTableWriter &btw, string dbName, string tableName,
	TableSP data) {
	DBConnection conn;
	conn.connect(hostName, port, "admin", "123456");
	size_t dataRow = data->rows();
	size_t dataColumn = data->columns();
	vector<VectorSP> dataVec;
	for (int i = 0; i < dataColumn; ++i) {
		dataVec.push_back(data->getColumn(i));
	}
	const int maxIndex = 10240;
	char boolBuffer[maxIndex];
	char charBuffer[maxIndex];
	short shortBuffer[maxIndex];
	char* stringBuffer[maxIndex];
	long long longBuffer[maxIndex];
	long long nanotimeBuffer[maxIndex];
	long long nanotimestampBuffer[maxIndex];
	long long timestampBuffer[maxIndex];
	float floatBuffer[maxIndex];
	double doubleBuffer[maxIndex];
	int intBuffer[maxIndex];
	int dateBuffer[maxIndex];
	int monthBuffer[maxIndex];
	int timeBuffer[maxIndex];
	int secondBuffer[maxIndex];
	int minuteBuffer[maxIndex];
	int datetimeBuffer[maxIndex];
	int datehourBuffer[maxIndex];
	int iidBuffer[maxIndex];
	const char *boolPtr = dataVec[0]->getBoolConst(0, maxIndex, boolBuffer);
	const char *charPtr = dataVec[1]->getCharConst(0, maxIndex, charBuffer);
	const short *shortPtr = dataVec[2]->getShortConst(0, maxIndex, shortBuffer);
	char **stringPtr = dataVec[3]->getStringConst(0, maxIndex, (char **)stringBuffer);
	const long long *longPtr = dataVec[4]->getLongConst(0, maxIndex, longBuffer);
	const long long *nanotimePtr = dataVec[5]->getLongConst(0, maxIndex, nanotimeBuffer);
	const long long *nanotimestampPtr = dataVec[6]->getLongConst(0, maxIndex, nanotimestampBuffer);
	const long long *timestampPtr = dataVec[7]->getLongConst(0, maxIndex, timestampBuffer);
	const float *floatPtr = dataVec[8]->getFloatConst(0, maxIndex, floatBuffer);
	const double *doublePtr = dataVec[9]->getDoubleConst(0, maxIndex, doubleBuffer);
	const int *intPtr = dataVec[10]->getIntConst(0, maxIndex, intBuffer);
	const int *datePtr = dataVec[11]->getIntConst(0, maxIndex, dateBuffer);
	const int *monthPtr = dataVec[12]->getIntConst(0, maxIndex, monthBuffer);
	const int *timePtr = dataVec[13]->getIntConst(0, maxIndex, timeBuffer);
	const int *secondPtr = dataVec[14]->getIntConst(0, maxIndex, secondBuffer);
	const int *minutePtr = dataVec[15]->getIntConst(0, maxIndex, minuteBuffer);
	const int *dateTimePtr = dataVec[16]->getIntConst(0, maxIndex, dateBuffer);
	const int *datehourPtr = dataVec[17]->getIntConst(0, maxIndex, datehourBuffer);
	const int *iidPtr = dataVec[18]->getIntConst(0, maxIndex, datehourBuffer);
	for (int i = 0; i < dataRow; ++i) {
		if (i % maxIndex == 0) {
			int getSize = min(i - i / maxIndex * maxIndex, maxIndex);
			boolPtr = dataVec[0]->getBoolConst(i, getSize, boolBuffer);
			charPtr = dataVec[1]->getCharConst(i, getSize, charBuffer);
			shortPtr = dataVec[2]->getShortConst(i, getSize, shortBuffer);
			stringPtr = dataVec[3]->getStringConst(i, getSize, (char **)stringBuffer);
			longPtr = dataVec[4]->getLongConst(i, getSize, longBuffer);
			nanotimePtr = dataVec[5]->getLongConst(i, getSize, nanotimeBuffer);
			nanotimestampPtr = dataVec[6]->getLongConst(i, getSize, nanotimestampBuffer);
			timestampPtr = dataVec[7]->getLongConst(i, getSize, timestampBuffer);
			floatPtr = dataVec[8]->getFloatConst(i, getSize, floatBuffer);
			doublePtr = dataVec[9]->getDoubleConst(i, getSize, doubleBuffer);
			intPtr = dataVec[10]->getIntConst(i, getSize, intBuffer);
			datePtr = dataVec[11]->getIntConst(i, getSize, dateBuffer);
			monthPtr = dataVec[12]->getIntConst(i, getSize, monthBuffer);
			timePtr = dataVec[13]->getIntConst(i, getSize, timeBuffer);
			secondPtr = dataVec[14]->getIntConst(i, getSize, secondBuffer);
			minutePtr = dataVec[15]->getIntConst(i, getSize, minuteBuffer);
			dateTimePtr = dataVec[16]->getIntConst(i, getSize, dateBuffer);
			datehourPtr = dataVec[17]->getIntConst(i, getSize, datehourBuffer);
			iidPtr = dataVec[18]->getIntConst(i, getSize, datehourBuffer);
		}
		btw.insert(dbName, tableName, Util::createInt(id),
			Util::createBool(boolPtr[i % maxIndex]),
			Util::createChar(charPtr[i % maxIndex]),
			Util::createShort(shortPtr[i%maxIndex]),
			Util::createString(string((char *)(stringPtr[i % maxIndex]))),
			Util::createLong(longPtr[i % maxIndex]),
			Util::createNanoTime(nanotimePtr[i % maxIndex]),
			Util::createNanoTimestamp(nanotimestampPtr[i % maxIndex]),
			Util::createTimestamp(timestampPtr[i % maxIndex]),
			Util::createFloat(floatPtr[i % maxIndex]),
			Util::createDouble(doublePtr[i % maxIndex]),
			Util::createInt(intPtr[i % maxIndex]),
			Util::createDate(datePtr[i % maxIndex]),
			Util::createMonth(monthPtr[i % maxIndex]),
			Util::createTime(timePtr[i % maxIndex]),
			Util::createSecond(secondPtr[i % maxIndex]),
			Util::createMinute(minutePtr[i % maxIndex]),
			Util::createDateTime(dateTimePtr[i % maxIndex]),
			Util::createDateHour(datehourPtr[i % maxIndex]),
			Util::createInt(iidPtr[i % maxIndex])
		);
	}
}

void test_batchTableWriter_insert_thread_fuction_using_cpp_type(int id, BatchTableWriter &btw, string dbName, string tableName,
	TableSP data) {
	DBConnection conn;
	conn.connect(hostName, port, "admin", "123456");
	size_t dataRow = data->rows();
	size_t dataColumn = data->columns();
	vector<VectorSP> dataVec;
	for (int i = 0; i < dataColumn; ++i) {
		dataVec.push_back(data->getColumn(i));
	}
	const int maxIndex = 10240;
	char boolBuffer[maxIndex];
	char charBuffer[maxIndex];
	short shortBuffer[maxIndex];
	char* stringBuffer[maxIndex];
	long long longBuffer[maxIndex];
	long long nanotimeBuffer[maxIndex];
	long long nanotimestampBuffer[maxIndex];
	long long timestampBuffer[maxIndex];
	float floatBuffer[maxIndex];
	double doubleBuffer[maxIndex];
	int intBuffer[maxIndex];
	int dateBuffer[maxIndex];
	int monthBuffer[maxIndex];
	int timeBuffer[maxIndex];
	int secondBuffer[maxIndex];
	int minuteBuffer[maxIndex];
	int datetimeBuffer[maxIndex];
	int datehourBuffer[maxIndex];
	int iidBuffer[maxIndex];
	char* stringCPPBuffer[maxIndex];
	const char *boolPtr = dataVec[0]->getBoolConst(0, maxIndex, boolBuffer);
	const char *charPtr = dataVec[1]->getCharConst(0, maxIndex, charBuffer);
	const short *shortPtr = dataVec[2]->getShortConst(0, maxIndex, shortBuffer);
	char **stringPtr = dataVec[3]->getStringConst(0, maxIndex, (char **)stringBuffer);
	const long long *longPtr = dataVec[4]->getLongConst(0, maxIndex, longBuffer);
	const long long *nanotimePtr = dataVec[5]->getLongConst(0, maxIndex, nanotimeBuffer);
	const long long *nanotimestampPtr = dataVec[6]->getLongConst(0, maxIndex, nanotimestampBuffer);
	const long long *timestampPtr = dataVec[7]->getLongConst(0, maxIndex, timestampBuffer);
	const float *floatPtr = dataVec[8]->getFloatConst(0, maxIndex, floatBuffer);
	const double *doublePtr = dataVec[9]->getDoubleConst(0, maxIndex, doubleBuffer);
	const int *intPtr = dataVec[10]->getIntConst(0, maxIndex, intBuffer);
	const int *datePtr = dataVec[11]->getIntConst(0, maxIndex, dateBuffer);
	const int *monthPtr = dataVec[12]->getIntConst(0, maxIndex, monthBuffer);
	const int *timePtr = dataVec[13]->getIntConst(0, maxIndex, timeBuffer);
	const int *secondPtr = dataVec[14]->getIntConst(0, maxIndex, secondBuffer);
	const int *minutePtr = dataVec[15]->getIntConst(0, maxIndex, minuteBuffer);
	const int *dateTimePtr = dataVec[16]->getIntConst(0, maxIndex, dateBuffer);
	const int *datehourPtr = dataVec[17]->getIntConst(0, maxIndex, datehourBuffer);
	const int *iidPtr = dataVec[18]->getIntConst(0, maxIndex, datehourBuffer);
	char **stringCPPPtr = dataVec[3]->getStringConst(0, maxIndex, (char **)stringCPPBuffer);
	for (int i = 0; i < dataRow; ++i) {
		if (i % maxIndex == 0) {
			int getSize = min(i - i / maxIndex * maxIndex, maxIndex);
			boolPtr = dataVec[0]->getBoolConst(i, getSize, boolBuffer);
			charPtr = dataVec[1]->getCharConst(i, getSize, charBuffer);
			shortPtr = dataVec[2]->getShortConst(i, getSize, shortBuffer);
			stringPtr = dataVec[3]->getStringConst(i, getSize, (char **)stringBuffer);
			longPtr = dataVec[4]->getLongConst(i, getSize, longBuffer);
			nanotimePtr = dataVec[5]->getLongConst(i, getSize, nanotimeBuffer);
			nanotimestampPtr = dataVec[6]->getLongConst(i, getSize, nanotimestampBuffer);
			timestampPtr = dataVec[7]->getLongConst(i, getSize, timestampBuffer);
			floatPtr = dataVec[8]->getFloatConst(i, getSize, floatBuffer);
			doublePtr = dataVec[9]->getDoubleConst(i, getSize, doubleBuffer);
			intPtr = dataVec[10]->getIntConst(i, getSize, intBuffer);
			datePtr = dataVec[11]->getIntConst(i, getSize, dateBuffer);
			monthPtr = dataVec[12]->getIntConst(i, getSize, monthBuffer);
			timePtr = dataVec[13]->getIntConst(i, getSize, timeBuffer);
			secondPtr = dataVec[14]->getIntConst(i, getSize, secondBuffer);
			minutePtr = dataVec[15]->getIntConst(i, getSize, minuteBuffer);
			dateTimePtr = dataVec[16]->getIntConst(i, getSize, dateBuffer);
			datehourPtr = dataVec[17]->getIntConst(i, getSize, datehourBuffer);
			iidPtr = dataVec[18]->getIntConst(i, getSize, datehourBuffer);
			stringCPPPtr = dataVec[3]->getStringConst(i, getSize, (char **)stringCPPBuffer);
		}
		btw.insert(dbName, tableName, id,
			boolPtr[i%maxIndex],
			charPtr[i%maxIndex],
			shortPtr[i%maxIndex],
			(const char*)(stringPtr[i%maxIndex]),
			longPtr[i%maxIndex],
			nanotimePtr[i%maxIndex],
			nanotimestampPtr[i%maxIndex],
			timestampPtr[i%maxIndex],
			floatPtr[i%maxIndex],
			doublePtr[i%maxIndex],
			intPtr[i%maxIndex],
			datePtr[i%maxIndex],
			monthPtr[i%maxIndex],
			timePtr[i%maxIndex],
			secondPtr[i%maxIndex],
			minutePtr[i%maxIndex],
			dateTimePtr[i%maxIndex],
			datehourPtr[i%maxIndex],
			iidPtr[i%maxIndex],
			string(stringCPPPtr[i%maxIndex])
		);
	}
}

bool stopFlag = false;

void batchTableWriter_thread_getAllStatus(BatchTableWriter &bts) {
	while (!stopFlag) {
		this_thread::sleep_for(chrono::milliseconds(100));
		TableSP t = bts.getAllStatus();
	}
}

void batchTableWriter_thread_getStatus(BatchTableWriter &bts, string dbName, string tableName) {
	while (!stopFlag) {
		this_thread::sleep_for(chrono::milliseconds(100));
		std::tuple<int, bool, bool> t = bts.getStatus(dbName, tableName);
	}
}

void
test_batchTableWriter_insert_unMultithread(BatchTableWriter &btw, string dbName, string tableName, bool partitionTable,
	string assertStr, int testRow) {
	string script;
	DBConnection connNew(false, false);
	connNew.connect(hostName, port, "admin", "123456");
	script += "login('admin', '123456');";
	script += "dbName = '" + dbName + "';";
	script += "tableName = '" + tableName + "';";
	if (tableName != "") {
		script += "dbPath = dbName;";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
		if (partitionTable)
			script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
		else
			script += "db.createTable(t,tableName).append!(t);";
	}
	else
		script +=
		"share table(100:0,`id`dbbool `dbchar `dbshort `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]) as " +
		dbName + ";";
	connNew.run(script);
	//string dbName = "dfs://test_batchTableWriter", tableName = "ptable";
	btw.addTable(dbName, tableName);
	//int testRow = 100000;
	string tableLocation = tableName != "" ? "loadTable('" + dbName + "', '" + tableName + "')" : dbName;
	connNew.run("testRow = " + to_string(testRow) + ";"
		"data = table(\n"
		"take([true, false, NULL], testRow) as dbbool, \n"
		"take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char() as dbchar, \n"
		"take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short() as dbshort, \n"
		"take(`AAA`BBB`CCC`中文, testRow) as dbstring, \n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow) as dblong, \n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotime() as dbnanotime,  \n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotimestamp() as dbnanotimestamp,  \n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).timestamp() as dbtimestamp,\n"
		"take(rand(10000000.0, 100).float().join(NULL), testRow) as dbfloat,\n"
		"take(rand(10000000.0, 100).join(NULL), testRow) as dbdouble,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).int() as dbint, \n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).date() as dbdate,\n"
		"take(rand(100, 100).join([pow(2,31)-1, NULL, 0]), testRow).month() as dbmonth , \n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).time() as dbtime, \n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).second() as dbsecond, \n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).minute() as dbminute, \n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datetime() as dbdatetime, \n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datehour() as dbdatehour, \n"
		"0.." + to_string(testRow - 1) + " as iid\n"
		");");
	TableSP data = connNew.run("select * from data order by iid");
	int testNum = 8;
	for (int i = 0; i < testNum; ++i) {
		test_batchTableWriter_insert_thread_fuction(i, ref(btw), dbName, tableName, data);
	}
	for(int j = 0; j < 200; ++j) {
		TableSP t = btw.getAllStatus();
		VectorSP sendedRows = t->getColumn(3);
		int size = sendedRows->size();
		int tmp[4];
		sendedRows->getInt(0, size, tmp);
		bool flag = true;
		for (int i = 0; i < size; ++i) {
			if (tmp[i] != testRow * testNum) {
				flag = false;
			}
		}
		if (flag)
			break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	if (tableName == "")
		connNew.run("share data as " + dbName + "Data");
	else
		connNew.run("share data as " + tableName + "Data");
}

void
test_batchTableWriter_insert_multithread_using_CPP_type(BatchTableWriter &btw, string dbName, string tableName, bool partitionTable,
	string assertStr, int testRow) {
	string script;
	DBConnection connNew(false, false);
	connNew.connect(hostName, port, "admin", "123456");
	script += "login('admin', '123456');";
	script += "dbName = '" + dbName + "';";
	script += "tableName = '" + tableName + "';";
	if (tableName != "") {
		script += "dbPath = dbName;";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid `CStinrg, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT, STRING]);";
		if (partitionTable)
			script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
		else
			script += "db.createTable(t,tableName).append!(t);";
	}
	else
		script +=
		"share table(100:0,`id`dbbool `dbchar `dbshort `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid`CStinrg, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT, STRING]) as " +
		dbName + ";";
	connNew.run(script);
	//string dbName = "dfs://test_batchTableWriter", tableName = "ptable";
	btw.addTable(dbName, tableName);
	//int testRow = 100000;
	string tableLocation = tableName != "" ? "loadTable('" + dbName + "', '" + tableName + "')" : dbName;
	connNew.run("testRow = " + to_string(testRow) + ";"
		"data = table(\n"
		"take([true, false, NULL], testRow) as dbbool,\n"
		"take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char() as dbchar,\n"
		"take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short() as dbshort,\n"
		"take(`AAA`BBB`CCC`中文, testRow) as dbstring,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow) as dblong,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotime() as dbnanotime,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotimestamp() as dbnanotimestamp,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).timestamp() as dbtimestamp,\n"
		"take(rand(10000000.0, 100).float().join(NULL), testRow) as dbfloat,\n"
		"take(rand(10000000.0, 100).join(NULL), testRow) as dbdouble,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).int() as dbint,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).date() as dbdate,\n"
		"take(rand(100, 100).join([pow(2,31)-1, NULL, 0]), testRow).month() as dbmonth ,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).time() as dbtime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).second() as dbsecond,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).minute() as dbminute,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datetime() as dbdatetime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datehour() as dbdatehour,\n"
		"0.." + to_string(testRow - 1) + " as iid,\n"
		"take(`AAA`BBB`CCC`中文, testRow) as CStinrg\n"
		");");
	TableSP data = connNew.run("select * from data order by iid");
	stopFlag = false;
	thread getAllStatus = thread(batchTableWriter_thread_getAllStatus, ref(btw));
	thread getStatus = thread(batchTableWriter_thread_getStatus, ref(btw), dbName, tableName);
	const int threadNum = 8;
	thread threadVec[threadNum];
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_thread_fuction_using_cpp_type, i, ref(btw), dbName, tableName, data);
	}
	for(int j = 0; j < 200; ++j) {
		TableSP t = btw.getAllStatus();
		VectorSP sendedRows = t->getColumn(3);
		int size = sendedRows->size();
		int tmp[4];
		sendedRows->getInt(0, size, tmp);
		bool flag = true;
		for (int i = 0; i < size; ++i) {
			if (tmp[i] != testRow * threadNum) {
				flag = false;
			}
		}
		if (flag)
			break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	if (tableName == "")
		connNew.run("share data as " + dbName + "Data");
	else
		connNew.run("share data as " + tableName + "Data");
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i].join();
	}
	stopFlag = true;
	//pthread_cancel(getAllStatus.native_handle());
	getAllStatus.join();
	//pthread_cancel(getStatus.native_handle());
	getStatus.join();
}

void test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread_using_CPP_type(ref(btw), "dfs://batchTableWriter", "batchTableWriter", true,
		"test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(i in 0..7){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid, CStinrg from loadTable(\"dfs://batchTableWriter\", \"batchTableWriter\") where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());"
		"flag.append!(equalRet);"
		"}; writeLog('cpp_api_test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_using_CPP_type", (bool)(ret->getBool()), true);
}

void
test_batchTableWriter_insert_multithread(BatchTableWriter &btw, string dbName, string tableName, bool partitionTable,
	string assertStr, int testRow) {
	string script;
	DBConnection connNew(false, false);
	connNew.connect(hostName, port, "admin", "123456");
	script += "login('admin', '123456');";
	script += "dbName = '" + dbName + "';";
	script += "tableName = '" + tableName + "';";
	if (tableName != "") {
		script += "dbPath = dbName;";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
		if (partitionTable)
			script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
		else
			script += "db.createTable(t,tableName).append!(t);";
	}
	else
		script +=
		"share table(100:0,`id`dbbool `dbchar `dbshort `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]) as " +
		dbName + ";";
	connNew.run(script);
	btw.addTable(dbName, tableName);
	string tableLocation = tableName != "" ? "loadTable('" + dbName + "', '" + tableName + "')" : dbName;
	connNew.run("testRow = " + to_string(testRow) + ";"
		"data = table(\n"
		"take([true, false, NULL], testRow) as dbbool,\n"
		"take((rand(100, 100) - 50).join([pow(2,7)-1, NULL, 0]), testRow).char() as dbchar,\n"
		"take((rand(100, 100) - 50).join([pow(2,15)-1, NULL, 0]), testRow).short() as dbshort,\n"
		"take(`AAA`BBB`CCC`中文, testRow) as dbstring,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow) as dblong,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotime() as dbnanotime,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).nanotimestamp() as dbnanotimestamp,\n"
		"take((rand(100, 100) - 50).long().join([pow(2,63)-1, NULL, 0]), testRow).timestamp() as dbtimestamp,\n"
		"take(rand(10000000.0, 100).float().join(NULL), testRow) as dbfloat,\n"
		"take(rand(10000000.0, 100).join(NULL), testRow) as dbdouble,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).int() as dbint,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).date() as dbdate,\n"
		"take(rand(100, 100).join([pow(2,31)-1, NULL, 0]), testRow).month() as dbmonth ,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).time() as dbtime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).second() as dbsecond,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).minute() as dbminute,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datetime() as dbdatetime,\n"
		"take((rand(100, 100) - 50).join([pow(2,31)-1, NULL, 0]), testRow).datehour() as dbdatehour,\n"
		"0.." + to_string(testRow - 1) + " as iid\n"
		");");

	TableSP data = connNew.run("select * from data order by iid");
	stopFlag = false;
	thread getAllStatus = thread(batchTableWriter_thread_getAllStatus, ref(btw));
	thread getStatus = thread(batchTableWriter_thread_getStatus, ref(btw), dbName, tableName);
	const int threadNum = 4;
	thread threadVec[threadNum];
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_thread_fuction, i, ref(btw), dbName, tableName, data);
	}
	for(int j = 0; j < 200; ++j) {
		TableSP t = btw.getAllStatus();
		VectorSP sendedRows = t->getColumn(3);
		int size = sendedRows->size();
		int tmp[4];
		sendedRows->getInt(0, size, tmp);
		bool flag = true;
		for (int i = 0; i < size; ++i) {
			if (tmp[i] != testRow * threadNum) {
				flag = false;
			}
		}
		if (flag)
			break;
		else {
			this_thread::sleep_for(chrono::seconds(1));
		}
	}
	if (tableName == "")
		connNew.run("share data as " + dbName + "Data");
	else
		connNew.run("share data as " + tableName + "Data");
	for (int i = 0; i < threadNum; ++i) {
		threadVec[i].join();
	}
	stopFlag = true;
	//pthread_cancel(getAllStatus.native_handle());
	getAllStatus.join();
	//pthread_cancel(getStatus.native_handle());
	getStatus.join();
}

void test_batchTableWriter_insert_unMultithread_dfsTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", false);
	thread threadVec[4];
	for (int i = 0; i < 4; ++i) {
		test_batchTableWriter_insert_unMultithread(ref(btw),
			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
			"test_batchTableWriter_insert_multithread_dfsTable" + to_string(i), 100000);
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(idnum in 0..3){"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\" + idnum, \"batchTableWriter\" + idnum ) where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());"
		"flag.append!(equalRet);"
		"}"
		"}; writeLog('cpp_api_test_batchTableWriter_insert_unMultithread_dfsTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_unMultithread_dfsTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_unMultithread_memoryTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", false);
	for (int i = 0; i < 4; ++i) {
		test_batchTableWriter_insert_unMultithread(ref(btw), "batchTableWriter" + to_string(i), "",
			true, "test_batchTableWriter_insert_multithread_memoryTable" + to_string(i), 100000);
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(idnum in 0..3){"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from objByName(\"batchTableWriter\" + idnum) where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"}"
		"}"
		"writeLog('cpp_api_test_batchTableWriter_insert_unMultithread_memoryTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_unMultithread_memoryTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_unMultithread_latitudeTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", false);
	for (int i = 0; i < 4; ++i) {
		test_batchTableWriter_insert_unMultithread(ref(btw),
			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
			"test_batchTableWriter_insert_multithread_latitudeTable" + to_string(i), 100000);
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(idnum in 0..3){"
		"for(i in 0..7){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\" + idnum, \"batchTableWriter\" + idnum ) where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());"
		"flag.append!(equalRet);"
		"}"
		"}"
		"writeLog('cpp_api_test_batchTableWriter_insert_unMultithread_latitudeTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_unMultithread_latitudeTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_multithread_one_dfsTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread(ref(btw), "dfs://batchTableWriter", "batchTableWriter", true,
		"test_batchTableWriter_insert_multithread_one_dfsTable", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\", \"batchTableWriter\") where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"};writeLog('cpp_api_test_batchTableWriter_insert_multithread_one_dfsTable = ' + flag.all()); flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_one_dfsTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_multithread_one_memoryTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread(ref(btw), "batchTableWriter", "", true,
		"test_batchTableWriter_insert_multithread_one_memoryTable", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from objByName(\"batchTableWriter\") where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"}"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_one_memoryTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_one_memoryTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_multithread_one_latitudeTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	test_batchTableWriter_insert_multithread(ref(btw), "dfs://batchTableWriter", "batchTableWriter", false,
		"test_batchTableWriter_insert_multithread_one_latitudeTable", 100000);
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\", \"batchTableWriter\") where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriterData\") order by iid).values());"
		"flag.append!(equalRet);"
		"};writeLog('cpp_api_test_test_batchTableWriter_insert_multithread_one_latitudeTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_one_latitudeTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_multithread_dfsTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	thread threadVec[4];
	for (int i = 0; i < 4; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_multithread, ref(btw),
			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
			"test_batchTableWriter_insert_multithread_dfsTable" + to_string(i), 100000);
	}
	for (int i = 0; i < 4; ++i) {
		threadVec[i].join();
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(idnum in 0..3){"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\" + idnum, \"batchTableWriter\" + idnum ) where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"}"
		"};writeLog('cpp_api_test_batchTableWriter_insert_multithread_dfsTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_dfsTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_multithread_memoryTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	thread threadVec[4];
	for (int i = 0; i < 4; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_multithread, ref(btw), "batchTableWriter" + to_string(i), "",
			true, "test_batchTableWriter_insert_multithread_memoryTable" + to_string(i), 100000);
	}
	for (int i = 0; i < 4; ++i) {
		threadVec[i].join();
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(idnum in 0..3){"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from objByName(\"batchTableWriter\" + idnum) where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"}"
		"}"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_memoryTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_memoryTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_insert_multithread_latitudeTable() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	thread threadVec[4];
	for (int i = 0; i < 4; ++i) {
		threadVec[i] = thread(test_batchTableWriter_insert_multithread, ref(btw),
			"dfs://batchTableWriter" + to_string(i), "batchTableWriter" + to_string(i), true,
			"test_batchTableWriter_insert_multithread_latitudeTable" + to_string(i), 100000);
	}
	for (int i = 0; i < 4; ++i) {
		threadVec[i].join();
	}
	ConstantSP ret = conn.run("flag = array(BOOL, 0, 10);"
		"for(idnum in 0..3){"
		"for(i in 0..3){"
		"ret = select dbbool, dbchar, dbshort, dbstring, dblong, dbnanotime, dbnanotimestamp, dbtimestamp, dbfloat, dbdouble, dbint, dbdate, dbmonth, dbtime, dbsecond,dbminute, dbdatetime,  dbdatehour, iid from loadTable(\"dfs://batchTableWriter\" + idnum, \"batchTableWriter\" + idnum ) where id = i order by iid;"
		"equalRet = eqObj((select * from ret order by iid).values(), (select * from objByName(\"batchTableWriter\" + idnum + \"Data\") order by iid).values());  \n"
		"flag.append!(equalRet);"
		"}"
		"}"
		"writeLog('cpp_api_test_batchTableWriter_insert_multithread_latitudeTable = ' + flag.all());flag.all();");
	if (assertObj)
		ASSERTION("test_batchTableWriter_insert_multithread_latitudeTable", (bool)(ret->getBool()), true);
}

void test_batchTableWriter_getAllStatus() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as teststatus;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	TableSP status = btw.getAllStatus();
	vector<string> colName;
	int size = status->columns();
	for (int i = 0; i < size; ++i) {
		colName.push_back(status->getColumnName(i));
	}
	if (colName[0] != "DatabaseName")
		ASSERTION("test_batchTableWriter_getAllStatus_1", colName[0], string("DatabaseName"));
	if (colName[1] != "TableName")
		ASSERTION("test_batchTableWriter_getAllStatus_2", colName[1], string("TableName"));
	if (colName[2] != "WriteQueueDepth")
		ASSERTION("test_batchTableWriter_getAllStatus_3", colName[2], string("WriteQueueDepth"));
	if (colName[3] != "SendedRows")
		ASSERTION("test_batchTableWriter_getAllStatus_4", colName[3], string("SendedRows"));
	if (colName[4] != "Removing")
		ASSERTION("test_batchTableWriter_getAllStatus_5", colName[4], string("Removing"));
	if (colName[5] != "Finished")
		ASSERTION("test_batchTableWriter_getAllStatus_6", colName[5], string("Finished"));
	if (status->getColumn(0)->getType() != DT_STRING)
		ASSERTION("test_batchTableWriter_getAllStatus_7", status->getColumn(0)->getType(), DT_STRING);
	if (status->getColumn(1)->getType() != DT_STRING)
		ASSERTION("test_batchTableWriter_getAllStatus_8", status->getColumn(1)->getType(), DT_STRING);
	if (status->getColumn(2)->getType() != DT_INT)
		ASSERTION("test_batchTableWriter_getAllStatus_9", status->getColumn(2)->getType(), DT_INT);
	if (status->getColumn(3)->getType() != DT_INT)
		ASSERTION("test_batchTableWriter_getAllStatus_10", status->getColumn(3)->getType(), DT_INT);
	if (status->getColumn(4)->getType() != DT_BOOL)
		ASSERTION("test_batchTableWriter_getAllStatus_11", status->getColumn(4)->getType(), DT_BOOL);
	if (status->getColumn(5)->getType() != DT_BOOL)
		ASSERTION("test_batchTableWriter_getAllStatus_12", status->getColumn(5)->getType(), DT_BOOL);
}

void test_batchTableWriter_getStatus() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as teststatus;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	try {
		std::tuple<int, bool, bool> status = btw.getStatus("dfs://test_batchTableWriter_no_exist", "");
		ASSERTION("test_batchTableWriter_getStatus", false, true);
	}
	catch (exception& e) {}
}

void test_fuc_addTable(BatchTableWriter &btw, string dbName, string tableName, int cycles) {
	for (int i = 0; i < cycles; ++i) {
		try {
			btw.addTable(dbName, tableName);
		}
		catch (exception &e) {}
	}
}

void test_fuc_removeTable(BatchTableWriter &btw, string dbName, string tableName, int cycles) {
	for (int i = 0; i < cycles; ++i) {
		try {
			btw.removeTable(dbName, tableName);
		}
		catch (exception &e) {}
	}
}

void test_fuc_getStatus(BatchTableWriter &btw, string dbName, string tableName, int cycles) {
	for (int i = 0; i < cycles; ++i) {
		try {
			btw.getStatus(dbName, tableName);
		}
		catch (exception &e) {}
	}
}

void test_fuc_getAllStatus(BatchTableWriter &btw, int cycles) {
	for (int i = 0; i < cycles; ++i) {
		try {
			btw.getAllStatus();
		}
		catch (exception &e) {}
	}
}

void test_multithread() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "share table(100:0,`test1 `test2, [INT,INT]) as teststatus;";
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	string dbName = "dfs://test_batchTableWriter";
	string tableName = "test";
	int cycles = 10000;
	thread t1(test_fuc_addTable, ref(btw), dbName, tableName, cycles);
	thread t2(test_fuc_removeTable, ref(btw), dbName, tableName, cycles);
	thread t3(test_fuc_getStatus, ref(btw), dbName, tableName, cycles);
	thread t4(test_fuc_getAllStatus, ref(btw), cycles);
	thread t5(test_fuc_addTable, ref(btw), dbName, tableName, cycles);
	thread t6(test_fuc_removeTable, ref(btw), dbName, tableName, cycles);
	thread t7(test_fuc_getStatus, ref(btw), dbName, tableName, cycles);
	thread t8(test_fuc_getAllStatus, ref(btw), cycles);
	t1.join();
	t2.join();
	t3.join();
	t4.join();
	t5.join();
	t6.join();
	t7.join();
	t8.join();
}

void test_BatchTableWriter_creat_network() {
	//broken
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	//no exception
}

void test_BatchTableWriter_addTable_network() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "tableName = 'BatchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,1..100);";
	script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
	script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
	DBConnection newConn(false, false);
	newConn.connect(hostName, port, "admin", "123456");
	newConn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	try {//broken
		btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
		cout << "FAIL--test_BatchTableWriter_addTable_network1" << endl;

	}
	catch (exception &e) {
		cout << "PASSED--test_BatchTableWriter_addTable_network1" << endl;
		cout << e.what() << endl;
		//what():  Failed to connect to server.
	}
	try {//connect
		btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
	}
	catch (exception &e) {
		cout << e.what() << endl;
		//what():  Failed to add table, the specified table has not been removed yet.
		cout << "FAIL--test_BatchTableWriter_addTable_network2" << endl;
	}
	try {//connect
		if (btw.getAllStatus()->rows() != 1)
			throw RuntimeException("The allStatus should have 1 row");
		try {
			btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
			cout << "FAIL--test_BatchTableWriter_addTable_network3-1" << endl;
		}
		catch (exception& e) {
			cout << "PASSED--test_BatchTableWriter_addTable_network3-1" << endl;
		}
		btw.removeTable("dfs://test_batchTableWriter", "BatchTableWriter");
		if (btw.getAllStatus()->rows() != 0)
			throw RuntimeException("The allStatus should have 0 row");
		string script;
		script += "login('admin', '123456');";
		script += "dbPath = 'dfs://test_batchTableWriter';";
		script += "tableName = 'BatchTableWriter';";
		script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
		script += "db=database(dbPath,VALUE,1..100);";
		script += "t=table(100:0,`id`dbbool `dbchar `dbshort  `dbstring `dblong `dbnanotime `dbnanotimestamp `dbtimestamp `dbfloat `dbdouble `dbint `dbdate `dbmonth `dbtime `dbsecond `dbminute `dbdatetime `dbdatehour `iid, [INT, BOOL, CHAR, SHORT, STRING, LONG, NANOTIME, NANOTIMESTAMP, TIMESTAMP, FLOAT, DOUBLE, INT, DATE, MONTH, TIME, SECOND, MINUTE, DATETIME, DATEHOUR, INT]);";
		script += "db.createPartitionedTable(t,tableName,`id).append!(t);";

		DBConnection newConn(false, false);
		newConn.connect(hostName, port, "admin", "123456");
		newConn.run(script);
		btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
		cout << "PASSED--test_BatchTableWriter_addTable_network3" << endl;
	}
	catch (exception &e) {
		cout << e.what() << endl;
		cout << "FAIL--test_BatchTableWriter_addTable_network3" << endl;
	}
}

void test_BatchTableWriter_remove_exception() {
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.removeTable("dfs://test_batchTableWriter", "BatchTableWriter");
	btw.removeTable("dfs://test_batchTableWriter", "BatchTableWriter");
	btw.removeTable("BatchTableWriter", "");
	//no exception
}

void test_BatchTableWriter_insert_network() {
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "tableName = 'BatchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,1..100);";
	script += "t=table(100:0,`id`dbbool, [INT, BOOL]);";
	script += "db.createPartitionedTable(t,tableName,`id).append!(t);";

	DBConnection newConn(false, false);
	newConn.connect(hostName, port, "admin", "123456");
	newConn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
	try {//broken
		 //while (true)
		btw.insert("dfs://test_batchTableWriter", "BatchTableWriter", 1, char(1));
	}
	catch (exception &e) {
		cout << e.what() << endl;
		if (btw.getAllStatus()->rows() != 0) {
			cout << "FAIL--test_BatchTableWriter_insert_network" << endl;
		}
	}
}

long long test_BatchTableWriter_insertNum = 0;

void test_BatchTableWriter_insert_fuc(BatchTableWriter &btw) {
	test_BatchTableWriter_insertNum = 0;
	try {
		while (true) {
			btw.insert("dfs://test_batchTableWriter", "BatchTableWriter", 1, 1);
			++test_BatchTableWriter_insertNum;
		}
	}
	catch (exception &e) {
		cout << e.what() << endl;
	}
}

void test_BatchTableWriter_remove_and_insert() {
	//connect
	string script;
	script += "login('admin', '123456');";
	script += "dbPath = 'dfs://test_batchTableWriter';";
	script += "tableName = 'BatchTableWriter';";
	script += "if(existsDatabase(dbPath)){dropDatabase(dbPath)};";
	script += "db=database(dbPath,VALUE,1..100);";
	script += "t=table(100:0,`id`dbbool, [INT, INT]);";
	script += "db.createPartitionedTable(t,tableName,`id).append!(t);";
	DBConnection conn(false, false);
	conn.connect(hostName, port, "admin", "123456");
	conn.run(script);
	BatchTableWriter btw(hostName, port, "admin", "123456", true);
	btw.addTable("dfs://test_batchTableWriter", "BatchTableWriter");
	thread t(test_BatchTableWriter_insert_fuc, ref(btw));
	ConstantSP ret = conn.run("exec count(*) from loadTable(dbPath, tableName)");
	//broke
	this_thread::sleep_for(chrono::seconds(1));
	if (ret->getLong() != test_BatchTableWriter_insertNum)
		ASSERTION("test_BatchTableWriter_remove_and_insert1", ret->getLong(), test_BatchTableWriter_insertNum);
	tuple<int, bool, bool> status = btw.getStatus("dfs://test_batchTableWriter", "BatchTableWriter");
	if (get<2>(status))
		ASSERTION("test_BatchTableWriter_remove_and_insert2", get<2>(status), true);
	t.join();
}

void test_BatchTableWriter_exception() {
	test_BatchTableWriter_creat_network();
	test_BatchTableWriter_addTable_network();
	test_BatchTableWriter_remove_exception();
	test_BatchTableWriter_insert_network();
	test_BatchTableWriter_remove_and_insert();
}

void test_BatchTableWriter_insert_error_type(string destType, BatchTableWriter& btw) {
	string passedStr = "PASSED--test_BatchTableWriter_insert_error_type_";
	string FAILStr = "FAIL--test_BatchTableWriter_insert_error_type_";
	if (destType != "BOOL") {
		try {
			if (destType != "CHAR") {
				btw.insert("batchTableWriter", "", char(1));
				cout << FAILStr + "BOOL_to_" + destType << endl;
			}
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createBool(char(1)));
			cout << FAILStr + "BOOL_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "CHAR") {
		try {
			if (destType != "BOOL") {
				btw.insert("batchTableWriter", "", char(1));
				cout << FAILStr + "CHAR_to_" + destType << endl;
			}
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createChar(char(1)));
			cout << FAILStr + "CHAR_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "SHORT") {
		try {
			btw.insert("batchTableWriter", "", short(1));
			cout << FAILStr + "SHORT_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createShort(short(1)));
			cout << FAILStr + "SHORT_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "STRING") {
		string str = "test";
		try {
			btw.insert("batchTableWriter", "", str);
			cout << FAILStr + "STRING_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", str.c_str());
			cout << FAILStr + "STRING_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createString(str));
			cout << FAILStr + "STRING_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "LONG") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "LONG_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createLong(long(1)));
			cout << FAILStr + "LONG_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "NANOTIME") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "NANOTIME_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createNanoTime(long(1)));
			cout << FAILStr + "NANOTIME_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "NANOTIMESTAMP") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "NANOTIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createNanoTimestamp(long(1)));
			cout << FAILStr + "NANOTIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "TIMESTAMP") {
		try {
			btw.insert("batchTableWriter", "", long(1));
			cout << FAILStr + "TIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createTimestamp(long(1)));
			cout << FAILStr + "TIMESTAMP_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "FLOAT") {
		try {
			btw.insert("batchTableWriter", "", float(1));
			cout << FAILStr + "FLOAT_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createFloat(float(1)));
			cout << FAILStr + "FLOAT_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DOUBLE") {
		try {
			btw.insert("batchTableWriter", "", double(1));
			cout << FAILStr + "DOUBLE_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDouble(double(1)));
			cout << FAILStr + "DOUBLE_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "INT") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "INT_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createInt(int(1)));
			cout << FAILStr + "INT_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DATE") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "DATE_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDate(int(1)));
			cout << FAILStr + "DATE_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "MONTH") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "MONTH_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createMonth(int(1)));
			cout << FAILStr + "MONTH_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "TIME") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "TIME_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createTime(int(1)));
			cout << FAILStr + "TIME_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "SECOND") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "SECOND_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createSecond(int(1)));
			cout << FAILStr + "SECOND_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "MINUTE") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "MINUTE_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createMinute(int(1)));
			cout << FAILStr + "MINUTE_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DATETIME") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "DATETIME_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDateTime(int(1)));
			cout << FAILStr + "DATETIME_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
	else if (destType != "DATEHOUR") {
		try {
			btw.insert("batchTableWriter", "", int(1));
			cout << FAILStr + "DATEHOUR_to_" + destType << endl;
		}
		catch (exception& e) {}
		try {
			btw.insert("batchTableWriter", "", Util::createDateHour(int(1)));
			cout << FAILStr + "DATEHOUR_to_" + destType << endl;
		}
		catch (exception& e) {}
	}
}

void test_BatchTableWriter_insert_error_type() {
	vector<string> type = { "BOOL", "CHAR","SHORT", "STRING", "LONG", "NANOTIME","NANOTIMESTAMP", "TIMESTAMP","FLOAT", "DOUBLE","INT", "DATE","MONTH", "TIME","SECOND", "MINUTE","DATETIME", "DATEHOUR", "IPADDR", "INT128", "SYMBOL" };
	for (int i = 0; i < 18; ++i) {
		string script;
		script += "share table(100:0,[`test], [" + type[i] + "]) as batchTableWriter;";
		conn.run(script);
		BatchTableWriter btw(hostName, port, "admin", "123456", true);
		btw.addTable("batchTableWriter", "");
		test_BatchTableWriter_insert_error_type(type[i], ref(btw));
	}
	string script;
	conn.run(script);
}

int main(int argc, char ** argv) {
	DBConnection::initialize();
	bool ret = conn.connect(hostName, port, "admin", "123456");
	if (!ret) {
		cout << "Failed to connect to the server" << endl;
		return 0;
	}
	else {
		cout << "connect to " + hostName + ":" + std::to_string(port);
	}

	int testVectorSize = 20;
	testClearMemory_var();
	testClearMemory_();
	test_AutoFitTableAppender_convert_to_datehour();
	testDatehourVector();
	testDatehourNullVector(testVectorSize);
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
	testSymbolBase();
	testSymbolSmall();
	testSymbolNull();
	testmixtimevectorUpload();
	testMemoryTable();
	testDFSTable();
	testDiskTable();
	testDFSTableSetStringWrong();
	testDFSTableSetString();
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
	////testshare();
	tets_Block_Reader_DFStable();
	test_Block_Table();
	test_block_skipALL();
	test_huge_table();
	test_huge_DFS();
	test_PartitionedTableAppender_value_int();
	test_PartitionedTableAppender_value_date1();
	test_PartitionedTableAppender_value_date2();
	test_PartitionedTableAppender_value_date3();
	test_PartitionedTableAppender_value_date4();
	test_PartitionedTableAppender_value_month1();
	test_PartitionedTableAppender_value_month2();
	test_PartitionedTableAppender_value_month3();
	test_PartitionedTableAppender_value_string();
	test_PartitionedTableAppender_value_symbol();
	test_PartitionedTableAppender_range_int();
	test_PartitionedTableAppender_range_date();
	test_PartitionedTableAppender_range_symbol();
	test_PartitionedTableAppender_range_string();
	test_PartitionedTableAppender_hash_int();
	test_PartitionedTableAppender_hash_date();
	test_PartitionedTableAppender_hash_symbol();
	test_PartitionedTableAppender_hash_string();
	test_PartitionedTableAppender_list_int();
	test_PartitionedTableAppender_list_date();
	test_PartitionedTableAppender_list_string();
	test_PartitionedTableAppender_list_symbol();
	test_PertitionedTableAppender_value_hash();
	test_PartitionedTableAppender_value_range();
	test_PartitionedTableAppender_compo3();
	test_symbol_optimization();
	test_AutoFitTableAppender_convert_to_date();
	test_AutoFitTableAppender_convert_to_month();
	test_AutoFitTableAppender_convert_to_datetime();
	test_AutoFitTableAppender_convert_to_timestamp();
	test_AutoFitTableAppender_convert_to_nanotimestamp();
	test_AutoFitTableAppender_convert_to_time();
	test_AutoFitTableAppender_convert_to_minute();
	test_AutoFitTableAppender_convert_to_second();
	test_AutoFitTableAppender_convert_to_nanotime();
	// test_AutoFitTableAppender_dfs_table();
	test_mutithread_basic();
	test_mutithread_WandR();
	test_mutithread_Session();
	test_batchTableWriter_insert();
	test_batchTableWriter_addTable();
	test_batchTableWriter_removeTable();
	test_batchTableWriter_getAllStatus();
	test_batchTableWriter_getStatus();
	test_batchTableWriter_insert_multithread_one_dfsTable();
	test_batchTableWriter_insert_multithread_one_memoryTable();
	test_batchTableWriter_insert_multithread_one_latitudeTable();
	test_batchTableWriter_insert_multithread_dfsTable_using_CPP_type();
	test_batchTableWriter_insert_unMultithread_dfsTable();
	test_batchTableWriter_insert_unMultithread_memoryTable();
	test_batchTableWriter_insert_unMultithread_latitudeTable();
	test_batchTableWriter_insert_multithread_dfsTable();
	test_batchTableWriter_insert_multithread_memoryTable();
	test_batchTableWriter_insert_multithread_latitudeTable();
	test_multithread();
	test_BatchTableWriter_insert_error_type();
	//test_BatchTableWriter_exception();
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
