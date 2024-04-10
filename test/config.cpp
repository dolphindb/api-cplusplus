#include "config.h"

string hostName = "127.0.0.1";
string errCode = "0";
int port = 8902;
int ctl_port = 8900;
string table = "trades";
vector<int> listenPorts = {18901, 18902, 18903, 18904, 18905, 18906, 18907, 18908, 18909, 18910};
unordered_set<int> usedPorts{};
string alphas = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
int pass, fail;
bool assertObj = true;
int vecSize = 20;

int const INDEX_MAX_1 = 1;
int const INDEX_MIN_2 = -1;

vector<string> sites = {"127.0.0.1:8902:datanode1", "127.0.0.1:8903:datanode2", "127.0.0.1:8904:datanode3"};
vector<string> backupSites = {"127.0.0.1:8902", "127.0.0.1:8903", "127.0.0.1:8904"};
string raftsGroup = "11";
vector<string> nodeNames = {"datanode1", "datanode2", "datanode3"};

DBConnection conn(false, false);
DBConnection connReconn(false, false);
DBConnection conn_compress(false, false, 7200, true);
DBConnectionSP connsp(new DBConnection());

// check server version
bool isNewServer(DBConnection &conn, const int &major, const int &minor, const int &revision){
    VectorSP serverVersion = conn.run("int(version().split(' ')[0].split('.')).nullFill(0)");
    int serverMajor = serverVersion->get(0)->getInt();
    int serverMinor = serverVersion->get(1)->getInt();
    int serverRevision = serverVersion->get(2)->getInt();
    if(serverMajor > major || (serverMajor == major && serverMinor > minor) || (serverMajor == major && serverMinor == minor && serverRevision >= revision))
        return true;
    return false;
};

void checkAPIVersion()
{
    const char *api_version = std::getenv("API_VERSION");
    if (api_version == nullptr)
    {
        cout << "API_VERSION is not set, skip version check\n";
        return;
    }

    if (api_version == "")
    {
        cout << "API_VERSION is not set, skip version check\n";
        return;
    }

    string actual_version = Util::VER;
    if (string(api_version) == actual_version)
    {
        cout << "version check successfully" << endl;
    }
    else
    {
        throw RuntimeException("API_VERSION is not match to the env");
    }
}

string getRandString(int len){
    unsigned int seed = (unsigned)time(NULL) ^ (unsigned)getpid();
    srand(seed);
    string str;
    for (int i = 0; i < len; i++)
    {
        str += alphas[rand() % alphas.size()];
    }
    return str;
}
