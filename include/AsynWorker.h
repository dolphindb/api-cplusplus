// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#include "Concurrent.h"
#include "DBConnectionPoolImpl.h"

namespace dolphindb {
class DBConnection;
class AsynWorker: public Runnable {
public:
    using Task = DBConnectionPoolImpl::Task;
    AsynWorker(DBConnectionPoolImpl& pool, CountDownLatchSP latch, const SmartPointer<DBConnection>& conn,
               const SmartPointer<SynchronizedQueue<Task>>& queue, TaskStatusMgmt& status,
               const std::string& hostName, const std::string& userId , const std::string& password)
            : pool_(pool), latch_(latch), conn_(conn), queue_(queue),taskStatus_(status),
              hostName_(hostName), userId_(userId), password_(password){}
protected:
    virtual void run();

private:
    DBConnectionPoolImpl& pool_;
    CountDownLatchSP latch_;
    SmartPointer<DBConnection> conn_;
	SmartPointer<SynchronizedQueue<Task>> queue_;
    TaskStatusMgmt& taskStatus_;
    const std::string hostName_;
    const std::string userId_;
    const std::string password_;
};

}