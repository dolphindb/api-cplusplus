/*
 * Concurrent.cpp
 *
 *  Created on: Jan 26, 2013
 *      Author: dzhou
 */

#include <iostream>
#include <chrono>

#ifdef MAC
	#include <errno.h>
#endif

#include "Concurrent.h"
#include "Exceptions.h"

namespace dolphindb {

Runnable::Runnable():status_(0){}

Runnable::~Runnable(){}

void Runnable::start(){
	status_ = 1;
	run();
	status_ = 2;
}

bool Runnable::isRunning(){
	return status_.load() == 1;
}

bool Runnable::isStarted(){
	return status_.load() >= 1;
}

bool Runnable::isComplete(){
	return status_.load() == 2;
}

Mutex::Mutex(){
#ifdef WINDOWS
	InitializeCriticalSection(&mutex_);
#else
	pthread_mutexattr_init(&attr_);
	pthread_mutexattr_settype(&attr_, PTHREAD_MUTEX_RECURSIVE);
	pthread_mutex_init(&mutex_, &attr_);
#endif
}

Mutex::~Mutex(){
#ifdef WINDOWS
	DeleteCriticalSection(&mutex_);;
#else
	pthread_mutex_destroy(&mutex_);
	pthread_mutexattr_destroy(&attr_);
#endif
}

void Mutex::lock(){
#ifdef WINDOWS
	EnterCriticalSection(&mutex_);
#else
	pthread_mutex_lock(&mutex_);
#endif
}

bool Mutex::tryLock(){
#ifdef WINDOWS
	return TryEnterCriticalSection(&mutex_);
#else
	return pthread_mutex_trylock(&mutex_)==0;
#endif
}

void Mutex::unlock(){
#ifdef WINDOWS
	LeaveCriticalSection(&mutex_);
#else
	pthread_mutex_unlock(&mutex_);
#endif
}

RWLock::RWLock(){
#ifdef WINDOWS
	InitializeSRWLock(&lock_);
#else
	int rc = pthread_rwlock_init(&lock_, NULL);
	if(rc != 0)
		throw RuntimeException("Failed to initialize read write lock with errCode " + std::to_string(rc));
#endif
}

RWLock::~RWLock(){
#ifndef WINDOWS
	pthread_rwlock_destroy(&lock_);
#endif
}

void RWLock::acquireRead() {
#ifdef WINDOWS
	AcquireSRWLockShared(&lock_);
#else
	int rc;
lockagain:
	rc = pthread_rwlock_rdlock(&lock_);
	if (rc != 0) {
		if (rc == EAGAIN)
			goto lockagain;
		throw RuntimeException("Failed to acquire shared lock with errCode " + std::to_string(rc));
	}
#endif
}

void RWLock::acquireWrite() {
#ifdef WINDOWS
	AcquireSRWLockExclusive(&lock_);
#else
	int rc;
lockagain:
	rc = pthread_rwlock_wrlock(&lock_);
	if (rc != 0) {
		if (rc == EAGAIN)
			goto lockagain;
		throw RuntimeException("Failed to acquire exclusive lock with errCode " + std::to_string(rc));
	}
#endif
}

bool RWLock::tryAcquireRead(){
#ifdef WINDOWS
	return TryAcquireSRWLockShared(&lock_);
#else
	return pthread_rwlock_tryrdlock(&lock_) == 0;
#endif
}

bool RWLock::tryAcqurieWrite(){
#ifdef WINDOWS
	return TryAcquireSRWLockExclusive(&lock_);
#else
	return  pthread_rwlock_trywrlock(&lock_) == 0;
#endif
}

void RWLock::releaseRead(){
#ifdef WINDOWS
	ReleaseSRWLockShared(&lock_);
#else
	int rc = pthread_rwlock_unlock(&lock_);
	if(rc != 0)
		throw RuntimeException("Failed to release shared lock with errCode " + std::to_string(rc));
#endif
}

void RWLock::releaseWrite(){
#ifdef WINDOWS
	ReleaseSRWLockExclusive(&lock_);
#else
	int rc = pthread_rwlock_unlock(&lock_);
	if(rc != 0)
		throw RuntimeException("Failed to release exclusive lock with errCode " + std::to_string(rc));
#endif
}


ConditionalVariable::ConditionalVariable(){
#ifdef WINDOWS
	InitializeConditionVariable (&conditionalVariable_);
#else
	pthread_cond_init (&conditionalVariable_, NULL);
#endif
}

ConditionalVariable::~ConditionalVariable(){
#ifdef WINDOWS

#else
	pthread_cond_destroy(&conditionalVariable_);
#endif
}

void ConditionalVariable::wait(Mutex& mutex){
#ifdef WINDOWS
	SleepConditionVariableCS(&conditionalVariable_, &mutex.mutex_, INFINITE);
#else
	pthread_cond_wait(&conditionalVariable_, &mutex.mutex_);
#endif
}

bool ConditionalVariable::wait(Mutex& mutex, int milliSeconds){
#ifdef WINDOWS
	return SleepConditionVariableCS(&conditionalVariable_, &mutex.mutex_, milliSeconds);
#else
	struct timespec curTime;
	clock_gettime(CLOCK_REALTIME, &curTime);
	long long ns = curTime.tv_nsec + (long long)milliSeconds * 1000000ll;
	curTime.tv_sec += ns / 1000000000;
	curTime.tv_nsec = ns % 1000000000;
	return pthread_cond_timedwait(&conditionalVariable_, &mutex.mutex_, &curTime) == 0;
#endif
}

void ConditionalVariable::notify(){
#ifdef WINDOWS
	WakeConditionVariable(&conditionalVariable_);
#else
	pthread_cond_signal(&conditionalVariable_);
#endif
}

void ConditionalVariable::notifyAll(){
#ifdef WINDOWS
	WakeAllConditionVariable(&conditionalVariable_);
#else
	pthread_cond_broadcast(&conditionalVariable_);
#endif
}

void CountDownLatch::wait(){
	LockGuard<Mutex> lock(&mutex_);
	while (count_ > 0){
		condition_.wait(mutex_);
	}
}

bool CountDownLatch::wait(int milliseconds){
	LockGuard<Mutex> lock(&mutex_);
	long long expire = std::chrono::steady_clock::now().time_since_epoch()/std::chrono::nanoseconds(1) + milliseconds * 1000000ll;
	int remaining = milliseconds;
	while (count_ > 0 && remaining > 0){
		condition_.wait(mutex_, remaining);
		remaining = (expire - std::chrono::steady_clock::now().time_since_epoch()/std::chrono::nanoseconds(1) + 500000) / 1000000;
	}
	return count_ == 0;
}

void CountDownLatch::countDown(){
	LockGuard<Mutex> lock(&mutex_);
	--count_;
	if (count_ == 0){
		condition_.notifyAll();
	}
}

void CountDownLatch::clear(){
	LockGuard<Mutex> lock(&mutex_);
	count_ = 0;
	condition_.notifyAll();
}

bool CountDownLatch::resetCount(int count) {
	LockGuard<Mutex> lock(&mutex_);
	if(count_ > 0 || count <= 0)
		return false;
	count_ = count;
	return true;
}

int CountDownLatch::getCount() const{
	LockGuard<Mutex> lock(&mutex_);
	return count_;
}

#ifdef MAC
Mutex Semaphore::globalIdMutex_;
#endif
Semaphore::Semaphore(int resources){
	//if(resources < 1)
	//	throw RuntimeException("Semaphore resource number must be positive.");

#ifdef WINDOWS
	if (resources == 0) {
		sem_ = CreateSemaphore(NULL, 0, LONG_MAX, NULL);
	}
	else {
#ifdef UNICODE
		std::wstring text = std::to_wstring(resources) + L"_DDB_SEM";
		sem_ = CreateSemaphore(NULL, 0, LONG_MAX, text.data());
#else
		std::string text = std::to_string(resources) + "DDB_SEM_";
		sem_ = CreateSemaphore(NULL, 0, LONG_MAX, text.data());
#endif
	}
	if(sem_ == NULL)
		throw RuntimeException("Failed to create semaphore with error code " + std::to_string(GetLastError()));
#elif defined MAC
	// std::atomic<long long>
	string sem_name;
	{
		LockGuard<Mutex> guard(&globalIdMutex_);
		sem_name = std::to_string(sem_id_++);
	}
	
	sem_ = sem_open(sem_name.c_str(), O_CREAT, 0666, resources);
	if (sem_ == SEM_FAILED){
		int err = errno;
		throw RuntimeException("Failed to create semaphore with error code " + std::to_string(err) + "yyyyyyy");
	}
	// sem_ = *(sem_tmp);
#else
	int ret = sem_init(&sem_, 0,resources);
	if(ret != 0){
		int err = errno;
		throw RuntimeException("Failed to create semaphore with error code " + std::to_string(err));
	}
#endif
}

Semaphore::~Semaphore(){
#ifdef WINDOWS
	CloseHandle(sem_);
#elif defined MAC
	sem_close(sem_);
#else
	sem_destroy(&sem_);
#endif
}

void Semaphore::acquire(){
#ifdef WINDOWS
	DWORD ret = WaitForSingleObject(sem_, INFINITE);
	if(ret != WAIT_OBJECT_0)
		throw RuntimeException("Failed to acquire semaphore with error code " + std::to_string(GetLastError()));
#elif defined MAC
	int ret = sem_wait(sem_);
	if(ret != 0){
		int err = errno;
		throw RuntimeException("Failed to acquire semaphore with error code " + std::to_string(err));
	}
#else
	
	int ret = sem_wait(&sem_);
	if(ret != 0){
		int err = errno;
		throw RuntimeException("Failed to acquire semaphore with error code " + std::to_string(err));
	}
#endif
}

#ifdef MAC
unsigned long long GetClockTimeMS(void){
	unsigned long long msTime;
	struct timespec curTime;

	clock_gettime(CLOCK_MONOTONIC_RAW, &curTime);
	msTime = curTime.tv_sec;
	msTime *= 1000;
	msTime += curTime.tv_nsec/1000000;
	return msTime;
}

int SleepEx(int ms){
	struct timeval timeout;
	timeout.tv_sec = ms/1000;
	timeout.tv_usec = (ms%1000)*1000;
	if(-1 == select(0, NULL, NULL, NULL, &timeout)){
		return errno;
	}
	return 0;
}

int WaitEvent(sem_t *hEvent, unsigned int milliseconds){
	int ret;
	unsigned long long timeout = milliseconds;
	unsigned long long beg = GetClockTimeMS();
	do{
		if(0 == sem_trywait((sem_t *)hEvent)){
			return 0;
		}
		ret = errno;
		if(EAGAIN != ret){
			return ret;
		}
		if(GetClockTimeMS()-beg >= timeout){
			break;
		}
		SleepEx(1);
	}while(1);
	return ETIMEDOUT;
}
#endif

bool Semaphore::tryAcquire(int waitMilliSeconds){
#ifdef WINDOWS
	return WaitForSingleObject(sem_, waitMilliSeconds) == WAIT_OBJECT_0;
#elif defined MAC
	return WaitEvent(sem_, waitMilliSeconds) == 0;
#else
	if(waitMilliSeconds > 0){
		struct timespec curTime;
		clock_gettime(CLOCK_REALTIME, &curTime);
		long long ns = curTime.tv_nsec + (long long)waitMilliSeconds * 1000000ll;
		curTime.tv_sec += ns / 1000000000;
		curTime.tv_nsec = ns % 1000000000;
		return sem_timedwait(&sem_, &curTime) == 0;
	}else{
		return sem_trywait(&sem_) == 0;
	}
#endif
}

void Semaphore::release(){
#ifdef WINDOWS
	if(!ReleaseSemaphore(sem_, 1, NULL))
		throw RuntimeException("Failed to release semaphore with error code " + std::to_string(GetLastError()));
#elif defined MAC
	int ret = sem_post(sem_);
	if(ret != 0){
		int err = errno;
		throw RuntimeException("Failed to release semaphore with error code " + std::to_string(err));
	}
#else
	int ret = sem_post(&sem_);
	if(ret != 0){
		int err = errno;
		throw RuntimeException("Failed to release semaphore with error code " + std::to_string(err));
	}
#endif
}

Thread::Thread(const RunnableSP& run):run_(run){
#ifndef WINDOWS
	thread_ = 0;
	pthread_attr_init(&attr_);
	pthread_attr_setdetachstate(&attr_, PTHREAD_CREATE_JOINABLE);
#else
	thread_ = 0;
	threadId_ = 0;
#endif
}

Thread::~Thread(){
#ifndef WINDOWS
	pthread_attr_destroy(&attr_);
#endif
}


void Thread::setAffinity(int id) {
#ifdef WINDOWS
	SYSTEM_INFO SystemInfo;
	GetSystemInfo(&SystemInfo);
	if (id >= SystemInfo.dwNumberOfProcessors) {
		throw RuntimeException("Core id exceed limit " + std::to_string(SystemInfo.dwNumberOfProcessors));
	}
	if (SetThreadAffinityMask(thread_, 1 << id) == 0) {
		throw RuntimeException("BindCore failed, error code "+GetLastError());
	}
#else
	int cpus = 0;
	cpus = sysconf(_SC_NPROCESSORS_ONLN);
	if (id >= cpus) {
		throw RuntimeException("Core id exceed limit " + std::to_string(cpus));
	}
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(id, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask) == -1) {
		throw RuntimeException("BindCore failed, error code " + errno);
	}
#endif
}

void Thread::start(){
#ifdef WINDOWS
	thread_=CreateThread(NULL,0,(LPTHREAD_START_ROUTINE) startFunc,this,0,&threadId_);
	if(thread_==0){
		std::cout<<"Failed to create thread with error code "<<GetLastError()<<std::endl;
	}
#else
	int ret=pthread_create(&thread_, &attr_, startFunc, this);
	if(ret!=0){
		std::cout<<"Failed to create thread with return value: "<<ret<<std::endl;
	}
#endif
}

void Thread::join(){
#ifdef WINDOWS
	WaitForSingleObject(thread_,INFINITE);
#else
	pthread_join(thread_, NULL);
#endif
}

void Thread::sleep(int milliSeconds){
#ifdef WINDOWS
	Sleep(milliSeconds);
#else
	usleep(1000*milliSeconds);
#endif
}

int Thread::getID(){
#ifdef WINDOWS
	return GetCurrentThreadId();
#elif defined MAC
	return syscall(SYS_thread_selfid);
#else
	return syscall(__NR_gettid);
#endif
}

};


