#include "demo.h"


int main()
{
    std::thread th_pollingClient;
    std::thread th_threadedClient;
    std::thread th_threadedClient_batch;
    std::thread th_threadPooledClient;
    {
        string sub_to_host = "192.168.100.9";
        int port = 13802;
        string tableName = "trades_stream_HA";
        string action = "test_pollingClient9_HA";
        string dfs = "dfs://test_9_PollingClient_sub_HA";
        string dfs_table = "trades9_HA";
        th_pollingClient = thread(PollingClient_sub, sub_to_host, port, tableName, action, dfs, dfs_table);
    }
    {
        string sub_to_host = "192.168.100.8";
        int port = 13802;
        string tableName = "trades_stream_HA";
        string action = "test_threadedClient8_HA";
        string dfs = "dfs://test_8_threadedClient_sub_HA";
        string dfs_table = "trades8_HA";
        th_threadedClient = thread(ThreadedClient_sub, sub_to_host, port, tableName, action, dfs,dfs_table);

    }
    {
        string sub_to_host = "192.168.100.8";
        int port = 13802;
        string tableName = "trades_stream_HA";
        string action = "test_threadedClient8_batch_HA";
        string dfs = "dfs://test_8_threadedClient_sub_batch_HA";
        string dfs_table = "trades8_HA";
        th_threadedClient_batch = thread(ThreadedClient_sub_batch, sub_to_host, port, tableName, action, dfs,dfs_table);

    }
    {
        string sub_to_host = "192.168.100.7";
        int port = 13802;
        string tableName = "trades_stream_HA";
        string action = "test_threadPooledClient7_HA";
        string dfs = "dfs://test_7_ThreadPooledClient_sub_HA";
        string dfs_table = "trades7_HA";
        th_threadPooledClient = thread(ThreadPooledClient_sub, sub_to_host, port, tableName, action, dfs, dfs_table);
    }

    th_pollingClient.detach();
    th_threadedClient.detach();
    th_threadedClient_batch.detach();
    th_threadPooledClient.join();
    return 0;
}