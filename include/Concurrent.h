// SPDX-License-Identifier: Apache-2.0
// Copyright © 2018-2025 DolphinDB, Inc.
#pragma once

#ifdef _MSC_VER
#pragma warning( push )
#pragma warning( disable : 4251 )
#endif

#include "Exports.h"
#include "Platform.h"
#include "SmartPointer.h"
#include <algorithm>
#include <cassert>
#include <functional>
#include <memory>
#include <queue>
#include <vector>

namespace dolphindb {

class Thread;
class Runnable;
class CountDownLatch;
using ThreadSP = SmartPointer<Thread>;
using RunnableSP = SmartPointer<Runnable>;
using CountDownLatchSP = SmartPointer<CountDownLatch>;

class EXPORT_DECL Runnable{
public:
	Runnable();
	void start();
	virtual ~Runnable() = default;
	bool isRunning();
	bool isStarted();
	bool isComplete();

protected:
	virtual void run()=0;

private:
	std::atomic<char> status_;
};

class EXPORT_DECL Executor : public Runnable {
	using Func = std::function<void()>;

public:
	explicit Executor(Func f) : func_(std::move(f)) {};
	void run() override { func_(); };

private:
	Func func_;
};

class EXPORT_DECL Mutex{
public:
	Mutex();
	~Mutex();
	void lock();
	bool tryLock();
	void unlock();

private:

#ifdef _WIN32
	CRITICAL_SECTION mutex_;
#else
	pthread_mutexattr_t attr_;
	pthread_mutex_t mutex_;
#endif
	friend class ConditionalVariable;
};

class EXPORT_DECL RWLock{
public:
	RWLock();
	~RWLock();
	void acquireRead();
	void acquireWrite();
	void releaseRead();
	void releaseWrite();
	bool tryAcquireRead();
	bool tryAcquireWrite();
private:
#ifdef _WIN32
	SRWLOCK lock_;
#else
	pthread_rwlock_t lock_;
#endif
};

class EXPORT_DECL ConditionalVariable{
public:
	ConditionalVariable();
	~ConditionalVariable();
	void wait(Mutex& mutex);
	bool wait(Mutex& mutex, int milliSeconds);
	void notify();
	void notifyAll();

private:
#ifdef _WIN32
	CONDITION_VARIABLE conditionalVariable_;
#else
	pthread_cond_t conditionalVariable_;
#endif
};


template<class T>
class EXPORT_DECL LockGuard{
public:
	explicit LockGuard(T* res, bool acquireLock = true):res_(res){
		if(acquireLock)
			res_->lock();
	}

	void unlock(){
		if(res_ != nullptr){
			res_->unlock();
			res_ = nullptr;
		}
	}

	~LockGuard(){
		if(res_ != nullptr)
			res_->unlock();
	}
private:
	T* res_;
};

template<class T>
class TryLockGuard{
public:
	explicit TryLockGuard(T* res, bool acquireLock = true):res_(res) {
		if(acquireLock)
			locked_ = res_->tryLock();
	}
	~TryLockGuard(){
		if(locked_)
			res_->unlock();
	}
	bool isLocked(){
		return locked_;
	}
private:
	T* res_;
	bool locked_{false};
};

template<class T>
class EXPORT_DECL RWLockGuard{
public:
	RWLockGuard(T* res, bool exclusive, bool acquireLock = true):res_(res), exclusive_(exclusive), acquireLock_(acquireLock){
		if(res != nullptr && acquireLock_){
			if(exclusive_)
				res_->acquireWrite();
			else
				res_->acquireRead();
		}
	}

	void upgrade(){
		if(res_ != nullptr && acquireLock_){
			if(exclusive_)
				return;
			res_->releaseRead();
			res_->acquireWrite();
			exclusive_ = true;
		}
	}

	~RWLockGuard(){
		if(res_ != nullptr && acquireLock_){
			if(exclusive_)
				res_->releaseWrite();
			else
				res_->releaseRead();
		}
	}
private:
	T* res_;
	bool exclusive_;
	bool acquireLock_;
};

template<class T>
class TryRWLockGuard{
public:
	TryRWLockGuard(T* res, bool exclusive, bool acquireLock = true):res_(res), exclusive_(exclusive) {
		if(acquireLock){
			if(exclusive_)
				locked_ = res_->tryAcquireWrite();
			else
				locked_ = res_->tryAcquireRead();
		}
	}
	~TryRWLockGuard(){
		if(locked_){
			if(exclusive_)
				res_->releaseWrite();
			else
				res_->releaseRead();
		}
	}
private:
	T* res_;
	bool exclusive_;
	bool locked_{false};
};

class EXPORT_DECL CountDownLatch{
public:
	explicit CountDownLatch(int count) : count_(count){}
	void wait();
	bool wait(int milliseconds);
	void countDown();
	int getCount() const;
	bool resetCount(int count);
	void clear();

private:
	mutable Mutex mutex_;
	ConditionalVariable condition_;
	int count_;
};

class EXPORT_DECL Semaphore{
public:
	explicit Semaphore(int resources = 0);
	~Semaphore();
	void acquire();
	bool tryAcquire(int waitMilliSeconds = 0);
	void release();

private:
#ifdef _WIN32
	HANDLE sem_;
#elif defined MAC
	sem_t *sem_;
	// static std::atomic<long long> sem_id_;
	long long sem_id_;
	static Mutex globalIdMutex_;
#else
	sem_t sem_;
#endif
};

template<class T>
class SynchronizedQueue{
public:
	SynchronizedQueue() = default;
	void push(const T& item){
		LockGuard<Mutex> guard(&mutex_);
		items_.push(item);
		if(items_.size() == 1)
			empty_.notifyAll();
	}

	void pop(std::vector<T>& container, int n){
		LockGuard<Mutex> guard(&mutex_);
		int count = ((std::min))((int)items_.size(), n);
		while(count>0){
			container.push_back(items_.front());
			items_.pop();
			--count;
		}
	}

	bool pop(T& item){
		LockGuard<Mutex> guard(&mutex_);
		if(items_.empty())
			return false;
		item = items_.front();
		items_.pop();
		return true;
	}

	bool peek(T& item){
		LockGuard<Mutex> guard(&mutex_);
		if(items_.empty())
			return false;
		item = items_.front();
		return true;
	}

	void blockingPop(T& item){
		LockGuard<Mutex> guard(&mutex_);
		while(items_.empty())
			empty_.wait(mutex_);
		item = items_.front();
		items_.pop();
	}

	bool blockingPop(T& item, int milliSeconds){
		LockGuard<Mutex> guard(&mutex_);
		while(items_.empty()){
			if(!empty_.wait(mutex_, milliSeconds))
				return false;
		}
		item = items_.front();
		items_.pop();
		return true;
	}

	std::size_t size(){
		LockGuard<Mutex> guard(&mutex_);
		return items_.size();
	}

	void clear(){
		LockGuard<Mutex> guard(&mutex_);
		while(!items_.empty())
			items_.pop();
	}

	template<class Y>
	void removeItem(Y func){
		LockGuard<Mutex> guard(&mutex_);
		std::queue<T> newItem;
		if(items_.empty())
			return;
		while(!items_.empty()){
			T item = items_.front();
			items_.pop();
			if(!func(item))
				newItem.push(item);
		}
		items_.swap(newItem);
	}

private:
	std::queue<T> items_;
	Mutex mutex_;
	ConditionalVariable empty_;
};

class EXPORT_DECL Thread{
public:
	explicit Thread(const RunnableSP& run);
	~Thread();
	void start();
	void join();
	bool isRunning(){return run_.isNull() ? false : run_->isRunning();}
	bool isComplete() {return run_.isNull()? false : run_->isComplete();}
	bool isStarted() {return run_.isNull()? false : run_->isStarted();}
#ifdef __linux__
	int setAffinity(const std::vector<size_t>& cpu);
	pthread_t getHandle() const;
#endif

private:
	static void* startFunc(void* data){
		((Thread*)data)->run_->start();
		return data;
	}

	RunnableSP run_;
#ifdef _WIN32
	HANDLE thread_;
	DWORD threadId_;
#else
	pthread_t thread_;
	pthread_attr_t attr_;
#endif
};

class EXPORT_DECL Signal{
public:
	explicit Signal(bool signaled = false, bool resetAfterWait = false):signaled_(signaled), resetAfterWait_(resetAfterWait){};
	void set(){
		LockGuard<Mutex> lock(&mutex_);
		if(signaled_)
			return;
		signaled_ = true;
		notifier_.notifyAll();
	}
	void reset(){
		LockGuard<Mutex> lock(&mutex_);
		signaled_ = false;
	}
	bool isSignaled(){
		LockGuard<Mutex> lock(&mutex_);
		return signaled_;
	}
	bool tryWait(int ms){
		LockGuard<Mutex> lock(&mutex_);
		if (signaled_ == false) {
			notifier_.wait(mutex_, ms);
		}
		bool result = signaled_;
		if (resetAfterWait_)
			signaled_ = false;
		return result;
	}
	void wait(){
		LockGuard<Mutex> lock(&mutex_);
		if (signaled_ == false) {
			notifier_.wait(mutex_);
		}
		if (resetAfterWait_)
			signaled_ = false;
	}
private:
	bool signaled_, resetAfterWait_;
	Mutex mutex_;
	ConditionalVariable notifier_;
};

template <typename T>
class BlockingQueue {
public:
    explicit BlockingQueue(size_t maxItems)
        : buf_(new T[maxItems]), capacity_(maxItems), batchSize_(1), size_(0), head_(0), tail_(0) {}
    explicit BlockingQueue(size_t maxItems, size_t batchSize)
        : buf_(new T[maxItems]), capacity_(maxItems), batchSize_(batchSize), size_(0), head_(0), tail_(0) {}
	std::size_t size(){
		LockGuard<Mutex> guard(&lock_);
		return size_;
	}
    void push(const T &item) {
        lock_.lock();
        while (size_ >= capacity_) full_.wait(lock_);
        buf_[tail_] = item;
        tail_ = (tail_ + 1) % capacity_;
        ++size_;

        if (size_ == 1) empty_.notifyAll();
        if (size_ == batchSize_) batch_.notifyAll();
        lock_.unlock();
    }
    void emplace(T &&item) {
        lock_.lock();
        while (size_ >= capacity_) full_.wait(lock_);
        buf_[tail_] = std::move(item);
        tail_ = (tail_ + 1) % capacity_;
        ++size_;
        if (size_ == 1) empty_.notifyAll();
        if (size_ == batchSize_) batch_.notifyAll();
        lock_.unlock();
    }
    bool poll(T &item, int milliSeconds) {
        if (milliSeconds < 0) {
            pop(item);
            return true;
        }
        LockGuard<Mutex> guard(&lock_);
        while (size_ == 0) {
            if (!empty_.wait(lock_, milliSeconds)) return false;
        }
        item = std::move(buf_[head_]);
        buf_[head_] = T();
        head_ = (head_ + 1) % capacity_;
        --size_;
        full_.notifyAll();
        return true;
    }
    void pop(T &item) {
        lock_.lock();
        while (size_ == 0) empty_.wait(lock_);
        item = std::move(buf_[head_]);
        buf_[head_] = T();
        head_ = (head_ + 1) % capacity_;
        --size_;
        full_.notifyAll();
        lock_.unlock();
    }

    bool pop(std::vector<T> &items, int milliSeconds) {
        LockGuard<Mutex> guard(&lock_);
        if (size_ < batchSize_){
            batch_.wait(lock_, milliSeconds);
        }
        if(size_ == 0)
            return false;
        std::size_t n = std::min(batchSize_, size_);
        items.resize(n);
        for(std::size_t i = 0; i < n; i++){
            items[i] = std::move(buf_[head_]);
            buf_[head_] = T();
            head_ = (head_ + 1) % capacity_;
        }
        full_.notifyAll();
        size_ -= n;
        return true;
    }
private:
    std::unique_ptr<T[]> buf_;
    size_t capacity_;
    size_t batchSize_;
    size_t size_;
    size_t head_;
    size_t tail_;
    Mutex lock_;
    ConditionalVariable full_;
    ConditionalVariable empty_;
    ConditionalVariable batch_;
    
};

} // namespace dolphindb

#ifdef _MSC_VER
#pragma warning( pop )
#endif
