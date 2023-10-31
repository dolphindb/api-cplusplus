#include "demo.h"

int main()
{
    std::thread th_pollingClient;
    std::thread th_threadedClient;
    std::thread th_threadedClient_batch;
    std::thread th_threadPooledClient;
    {
        string sub_to_host = "192.168.100.7";
        int port = 13802;
        string tableName = "trades_stream7";
        string action = "test_pollingClient7";
        string dfs = "dfs://test_7_PollingClient_sub";
        string dfs_table = "trades7";
        th_pollingClient = thread(PollingClient_sub, sub_to_host, port, tableName, action, dfs, dfs_table);
    }
    {
        string sub_to_host = "192.168.100.9";
        int port = 13802;
        string tableName = "trades_stream9";
        string action = "test_threadedClient9";
        string dfs = "dfs://test_9_threadedClient_sub";
        string dfs_table = "trades9";
        th_threadedClient = thread(ThreadedClient_sub, sub_to_host, port, tableName, action, dfs,dfs_table);

    }
    {
        string sub_to_host = "192.168.100.9";
        int port = 13802;
        string tableName = "trades_stream9";
        string action = "test_threadedClient9_batch";
        string dfs = "dfs://test_9_threadedClient_sub_batch";
        string dfs_table = "trades9";
        th_threadedClient_batch = thread(ThreadedClient_sub_batch, sub_to_host, port, tableName, action, dfs,dfs_table);

    }
    {
        string sub_to_host = "192.168.100.8";
        int port = 13802;
        string tableName = "trades_stream8";
        string action = "test_threadPooledClient8";
        string dfs = "dfs://test_8_ThreadPooledClient_sub";
        string dfs_table = "trades8";
        th_threadPooledClient = thread(ThreadPooledClient_sub, sub_to_host, port, tableName, action, dfs, dfs_table);
    }

    th_pollingClient.detach();
    th_threadedClient.detach();
    th_threadedClient_batch.detach();
    th_threadPooledClient.join();
    return 0;
}