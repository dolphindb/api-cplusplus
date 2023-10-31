#include "demo.h"

int main()
{
    DBConnection conn(false, false);
    conn.connect("192.168.100.7", 13800, "admin", "123456");
    string s = "t1=table(1:0,`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber`bidTradeMaxDuration`offerTradeMaxDuration`numBidOrders`bnumOfferOrders`lastTradeTime`varietyCategory`receivedTime`dailyIndex,\
                [INT, SYMBOL, TIMESTAMP, STRING, \
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                INT, STRING, STRING,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                INT, INT, INT, INT, DOUBLE, CHAR, NANOTIMESTAMP, INT]);\
                t2=table(1:0,`marketType`securityCode`origTime`tradingPhaseCode`preClosePrice`openPrice`highPrice`lowPrice`lastPrice`closePrice`bidPrice1`bidPrice2`bidPrice3`bidPrice4`bidPrice5`bidPrice6`bidPrice7`bidPrice8`bidPrice9`bidPrice10`bidVolume1`bidVolume2`bidVolume3`bidVolume4`bidVolume5`bidVolume6`bidVolume7`bidVolume8`bidVolume9`bidVolume10`offerPrice1`offerPrice2`offerPrice3`offerPrice4`offerPrice5`offerPrice6`offerPrice7`offerPrice8`offerPrice9`offerPrice10`offerVolume1`offerVolume2`offerVolume3`offerVolume4`offerVolume5`offerVolume6`offerVolume7`offerVolume8`offerVolume9`offerVolume10`numTrades`totalVolumeTrade`totalValueTrade`totalBidVolume`totalOfferVolume`weightedAvgBidPrice`weightedAvgOfferPrice`ioPV`yieldToMaturity`highLimited`lowLimited`priceEarningRatio1`priceEarningRatio2`change1`change2`channelNo`mdStreamID`instrumentStatus`preCloseIOPV`altWeightedAvgBidPrice`altWeightedAvgOfferPrice`etfBuyNumber`etfBuyAmount`etfBuyMoney`etfSellNumber`etfSellAmount`etfSellMoney`totalWarrantExecVolume`warLowerPrice`warUpperPrice`withdrawBuyNumber`withdrawBuyAmount`withdrawBuyMoney`withdrawSellNumber`withdrawSellAmount`withdrawSellMoney`totalBidNumber`totalOfferNumber,\
                [INT, SYMBOL, TIMESTAMP, STRING, \
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE,\
                INT, STRING, STRING,\
                DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE, DOUBLE]);";
    conn.run(s);
    DictionarySP t1s = conn.run("schema(t1)");
    DictionarySP t2s = conn.run("schema(t2)");
    unordered_map<string, DictionarySP> sym2schema;
    sym2schema["msg1"] = t1s;
    sym2schema["msg2"] = t2s;
    StreamDeserializerSP sdsp = new StreamDeserializer(sym2schema);

    conn.close();

    std::thread th_pollingClient;
    std::thread th_threadedClient;
    std::thread th_threadedClient_batch;
    std::thread th_threadPooledClient;
    {
        string sub_to_host = "192.168.100.8";
        int port = 13802;
        string tableName = "trades_stream8_d";
        string action = "test_pollingClient8_d";
        vector<string> dbs = {"dfs://test_8_PollingClient_sub_d_1", "dfs://test_8_PollingClient_sub_d_2"};
        vector<string> dfstables = {"trades8_d", "trades8_d"};
        th_pollingClient = thread(PollingClient_sub_d, sub_to_host, port, tableName, action, dbs, dfstables, sdsp);
    }
    {
        string sub_to_host = "192.168.100.7";
        int port = 13802;
        string tableName = "trades_stream7_d";
        string action = "test_threadedClient7_d";
        vector<string> dbs = {"dfs://test_7_threadedClient_sub_d_1", "dfs://test_7_threadedClient_sub_d_2"};
        vector<string> dfstables = {"trades7_d", "trades7_d"};
        th_threadedClient = thread(ThreadedClient_sub_d, sub_to_host, port, tableName, action, dbs, dfstables, sdsp);
    }
    {
        string sub_to_host = "192.168.100.7";
        int port = 13802;
        string tableName = "trades_stream7_d";
        string action = "test_threadedClient7_batch_d";
        vector<string> dbs = {"dfs://test_7_threadedClient_sub_batch_d_1", "dfs://test_7_threadedClient_sub_batch_d_2"};
        vector<string> dfstables = {"trades7_d", "trades7_d"};
        th_threadedClient_batch = thread(ThreadedClient_sub_batch_d, sub_to_host, port, tableName, action, dbs, dfstables, sdsp);
    }
    {
        string sub_to_host = "192.168.100.9";
        int port = 13802;
        string tableName = "trades_stream9_d";
        string action = "test_threadPooledClient9_d";
        vector<string> dbs = {"dfs://test_9_ThreadPooledClient_sub_d_1", "dfs://test_9_ThreadPooledClient_sub_d_2"};
        vector<string> dfstables = {"trades9_d", "trades9_d"};
        th_threadPooledClient = thread(ThreadPooledClient_sub_d, sub_to_host, port, tableName, action, dbs, dfstables, sdsp);
    }

    th_pollingClient.detach();
    th_threadedClient.detach();
    th_threadedClient_batch.detach();
    th_threadPooledClient.join();
    return 0;
}