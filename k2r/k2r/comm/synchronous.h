/*
 * synchronous.h
 *
 *  Created on: 2015-07-11
 *      Author: liulin
 */

#ifndef CRITICALSECTION_H_
#define CRITICALSECTION_H_

#include <string>
#ifdef _WIN32
#include <Windows.h>
#define CRITICALSECTION CRITICAL_SECTION
#define SEMAPHORE HANDLE
#else // Ä¿Ç°Ä¬ÈÏºìÆì64
#include <pthread.h>
#include <semaphore.h>
#define CRITICALSECTION  pthread_mutex_t
#endif  //_WIN32


class CCriticalSec
{
private:
	CRITICALSECTION m_Section;
#ifndef _WIN32
	pthread_mutexattr_t m_mattr;
#endif
public:
	CCriticalSec();
	~CCriticalSec();
	int Lock();
	int UnLock();

};

class Mutex
{
#ifdef _WIN32
	HANDLE hMutex;
#else
	sem_t sem_id;
	sem_t* p_semid;
#endif
	
public:
	Mutex(void);
	Mutex(const char* name);
	virtual ~Mutex(void);
	bool take(int milsec = 0);
	void release();
};


#endif /* CRITICALSECTION_H_ */
