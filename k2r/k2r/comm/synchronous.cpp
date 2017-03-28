/*
 * synchronous.cpp
 *
 *  Created on: 2015-07-11
 *      Author: liulin
 */
#include "synchronous.h"
#include <assert.h>
#ifdef _WIN32


CCriticalSec::CCriticalSec()
{
	InitializeCriticalSection(&m_Section);
}

CCriticalSec::~CCriticalSec()
{
	DeleteCriticalSection(&m_Section);
}

int CCriticalSec::Lock()
{
	EnterCriticalSection(&m_Section);

	return 0;
}
                                         
int CCriticalSec::UnLock()
{
	LeaveCriticalSection(&m_Section);
	return 0;

}
#else
#include <semaphore.h>
#include <fcntl.h>
#include <errno.h>
#define FILE_MODE  (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
int CCriticalSec::UnLock()
{
	return pthread_mutex_unlock(&m_Section);//����
}



int CCriticalSec::Lock()
{
	return pthread_mutex_lock(&m_Section);
}



CCriticalSec::CCriticalSec()
{
	pthread_mutexattr_init(&m_mattr);
	pthread_mutexattr_setpshared(&m_mattr,PTHREAD_PROCESS_PRIVATE);//进程内可用
	pthread_mutexattr_settype(&m_mattr,PTHREAD_MUTEX_RECURSIVE);//同一线程可重复锁,同时需要多次打开
	pthread_mutex_init (&m_Section, &m_mattr);
}



CCriticalSec::~CCriticalSec()
{
	pthread_mutex_destroy(&m_Section);
	pthread_mutexattr_destroy(&m_mattr);
}
#endif




Mutex::Mutex(void)
{
#ifdef _WIN32
	//hMutex = CreateMutex(NULL,FALSE,NULL);
	hMutex = CreateSemaphore(NULL,1,1,NULL);
#else
	sem_init(&sem_id, 0, 1);
	p_semid = NULL;
#endif
}


Mutex::Mutex(const char* name)
{
#ifdef _WIN32
	//hMutex = CreateMutex(NULL,FALSE,name);
	hMutex = CreateSemaphore(NULL,1,1,name);
#else
	p_semid = sem_open(name, O_CREAT | O_EXCL, FILE_MODE, 0);
	//sem_id = 0; //hj修改 2016-2-28
#endif
}

Mutex::~Mutex(void)
{
#ifdef _WIN32
	CloseHandle(hMutex);
#else
	if(&sem_id) sem_destroy(&sem_id);
	if(p_semid) sem_unlink((const char*)p_semid);
#endif
}

bool Mutex::take(int milsec)
{
#ifdef _WIN32
	int ret = WaitForSingleObject(hMutex,milsec);
	assert(ret != -1);
	return (WAIT_OBJECT_0 == ret);
#else
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	ts.tv_nsec = 1000000*(milsec%1000);
	ts.tv_sec += milsec/1000 + ts.tv_nsec/1000000;
	ts.tv_nsec %= 1000000;
	sem_t* tmp = NULL;
	if(&sem_id) 
	{
		tmp = &sem_id;
	}
	if (p_semid)
	{
		tmp = p_semid;
	}
	int s = 0;
	while ( (s = sem_timedwait(tmp,&ts)) == -1 && errno == EINTR) continue;
	assert(s == -1 && errno != ETIMEDOUT);
	return (s != -1);
#endif
}

void Mutex::release()
{
#ifdef _WIN32
	//ReleaseMutex(hMutex);
	long pre = 0;
	ReleaseSemaphore(hMutex,1,&pre);
#else
	sem_t* tmp = NULL;
	if(&sem_id) 
	{
		tmp = &sem_id;
	}
	if (p_semid)
	{
		tmp = p_semid;
	}
	sem_post(tmp);
#endif
}
