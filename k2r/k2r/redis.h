
/*
redis.h
create on:2014-11-13
author:zclf
*/
#include<stdarg.h>
#include <string.h>
#include<string>
using namespace std;
#include<vector>

//by zx 20150203-------------------------------------------
#include"redisclient.h"
//---------------------------------------------------------

#ifndef REDIS_H_
#define REDIS_H_
struct data_redis {
	int deviceID;
	int attriID;
	string time;
	int status;
	float primaryData;
	float modifyData;
	float predictedData;
	string name;
};

inline int initSocket()
{
#ifdef WIN32
	//the version bit of Winsock      
	int version_a = 1;//low bit      
	int version_b = 1;//high bit       
	//makeword      
	WORD versionRequest = MAKEWORD(version_a,version_b);      
	WSAData wsaData;      
	int error;      
	error = WSAStartup(versionRequest, &wsaData);        
	if(error != 0)
	{          
		printf("ERROR!");          
		return -1;      
	}      
	//check whether the version is 1.1, if not print the error and cleanup wsa?      
	if (LOBYTE(wsaData.wVersion)  != 1 || HIBYTE(wsaData.wVersion) != 1)      
	{          
		printf("WRONG WINSOCK VERSION!");          
		WSACleanup();          
		return -1;      
	}
	return 1;
#else
	return 1;
#endif
}

class redisPublishClient {
public:
	redisPublishClient();
	~redisPublishClient();
	//by zx 20150420--------------------------------------------
	bool connect(const string ip,int port,bool bEnableReconnect = true);
	//----------------------------------------------------------
	bool send(const string channelName, string msg);
	bool close_();
	bool isconnect();
	//by zx 20150203-----------------------------------------
	boost::shared_ptr<redis::client> m_redisPublishClient;
	//-------------------------------------------------------
private:
	string m_ip;
};
class redisSubscribeChannelClient {
//by zx 20150203---------------------------------------------------
public:
	boost::shared_ptr<redis::client> m_redisSubscribeClient;
//-----------------------------------------------------------------
private:
	string m_ip;
	string m_channelName; //通道名字
public:
	redisSubscribeChannelClient();
	~redisSubscribeChannelClient();
	bool connect(const string ip,int port,bool bEnableReconnect = true);
	bool send(const string channelName);
	string receive();
	bool close_();
	bool isconnect();
};

class CRedisList {
public:
	boost::shared_ptr<redis::client> m_redisList;
	string m_ip;
private:
	//@func:获取定义时间(time_t)格式只字符串
	const char* timestamp(time_t* t=NULL);
	//@func:时间由字符串(String)转化为秒数(time_t)
	time_t TimeFromString(const char * strTime);
public:
	CRedisList();
	~CRedisList();
	//by zx 20150415------------------------------------------------
	bool connect(const string ip,int port,bool bEnableReconnect = true);
	//--------------------------------------------------------------
	bool write(vector<data_redis> &writeDataVector,std::string strKeyprefix);
	bool readMultiData(const string key, vector<string> &data, int startIndex,int endIndex);
	bool readSingleData(const string key, string &data, int index);
	bool writeBYNAME(vector<data_redis> writeDataVector,std::string strKeyprefix);
	bool GetAllKey(string pattern,vector<string>&key_name);
	bool GetLastTimeforMonth(string key,string &time);
	bool close();
};

class COperateData {
private:
	
	string m_ip;
	int m_port;
	vector<double>m_exception_data;
	int m_rate;

private:
	//@func:获取定义时间(time_t)格式只字符串
	const char * timestamp(time_t* t=NULL);
	//@func:时间由字符串(String)转化为秒数(time_t)
	time_t TimeFromString(const char * strTime);
public:
	CRedisList CRList;
	static COperateData *m_COperateData;

	COperateData();
	~COperateData();

	static COperateData *getInstance()
	{
		if (m_COperateData == NULL) {
			m_COperateData = new COperateData();
		}
		return m_COperateData;
	}
	//@初始化类的成员变量,ip为redis ip,port默认为6379
	bool InitOperateData(const string ip, int port,int rate);
	//@判断是否与redis 服务器连接
	bool Connect(bool bEnableReconnect = true);
	//@向redis中存入数据，key为name#id
	bool PushData(vector<data_redis> &write_data_vector,std::string strKeyprefix="");
	//@向redis中存入数据，key为name
	bool PushDataBYNAME(vector<data_redis> &write_data_vector,std::string strKeyprefix="");
	//@从redis中获取数据，key为device_id#attri_id
	bool GetData(int device_id, int attri_id, string start_time, string end_time,
		vector<data_redis>&data_vector);
	//@从redis中获取数据，key为name
	bool GetData(string name, string start_time, string end_time,
		vector<data_redis>&data_vector);
	//@修改redis中的数据信息，key为device_id#attri_id
	bool ModifyData(int device_id, int attri_id, vector<data_redis> modify_data);
	//@修改redis中的数据信息，key为name
	bool ModifyData(string name, vector<data_redis> modify_data);
	//@判断该属性在这个时刻是否存在，key为device_id#attri_id;
	bool IsExist(int device_id, int attri_id, string time, data_redis &point);
	//@判断该属性在这个时刻是否存在，key为name
	bool IsExist(string name, string time, data_redis &point);
	//@获取上一个时刻的值，key为device_id#attri_id
	bool GetLastTimeData(int device_id, int attri_id, string time, data_redis &point);
	//@获取上一个时刻的值，key为name
	bool GetLastTimeData(string name, string time, data_redis &point);
	bool GetDeviceName(string pattern,vector<string>&table_name);
	bool GetLastCorrectValue(string name,string time,double &val);
	bool Push(string key,string val);

	bool Get(string key,string &val);
	bool DeleteData(string key,int count);
	int  GetLenofKey(string key);
	bool PushMultiData(const vector<pair<string, string>>& mdata);
};

#endif /* REDIS_H_ */
