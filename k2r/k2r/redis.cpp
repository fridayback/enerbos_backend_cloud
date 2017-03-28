/*
 * redisclient.cpp
 *
 *  Created on: 2014年11月11日
 *      Author: zclf
 */
/*
 * redisclient.cpp
 *
 *  Created on: 2014年11月11日
 *      Author: zclf
 */

//#include"redisclient.h"
#include"redis.h"

using namespace boost;
//boost::shared_ptr<redis::client> m_redisPublishClient;
//boost::shared_ptr<redis::client> m_redisSubscribeClient;
//boost::shared_ptr<redis::client> m_redisList;



redisPublishClient::redisPublishClient() {
}
redisPublishClient::~redisPublishClient() {
}
bool redisPublishClient::connect(const string ip,int port,bool bEnableReconnect) {
	m_ip = ip;
	m_redisPublishClient = boost::shared_ptr<redis::client>(
			new redis::client(ip,port));
	//by zx 20150415---------------------------------------
	m_redisPublishClient->SetReConnecteEnableFlag(bEnableReconnect);
	//-----------------------------------------------------
	return true;
}
bool redisPublishClient::send(const string channelName, string msg) {
	m_redisPublishClient->publish(channelName, msg);
	return true;
}
bool redisPublishClient::close_() {
	return m_redisPublishClient->closeClient();

}

bool redisPublishClient::isconnect()
{
	return m_redisPublishClient->connectionCheck;
}

//接收频道消息客户端

redisSubscribeChannelClient::redisSubscribeChannelClient() {
}
redisSubscribeChannelClient::~redisSubscribeChannelClient() {
}
//by zx 20150415------------------------------------------------------------------------------------
bool redisSubscribeChannelClient::connect(const string ip,int port,bool bEnableReconnect) {
	m_ip = ip;
	m_redisSubscribeClient = boost::shared_ptr<redis::client>(
			new redis::client(ip,port));
	//by zx 20140415----------------------------------------------
	m_redisSubscribeClient->SetReConnecteEnableFlag(bEnableReconnect);
	//------------------------------------------------------------
	return m_redisSubscribeClient->connectionCheck;
}
bool redisSubscribeChannelClient::send(const string channelName) {
	m_channelName = channelName;
	if (m_redisSubscribeClient->subscribe(channelName) > 0)
		return true;
	return false;
}
string redisSubscribeChannelClient::receive() {

	while (1) {
		string recData = "";
		m_redisSubscribeClient->recvChannel(m_channelName, recData);
		if ((recData.find("$") == string::npos)
				&& (recData.find("subscribe") == string::npos)
				&& (recData.find(m_channelName) == string::npos)
				&& (recData.find(":") == string::npos)
				&& (recData.find("*") == string::npos)
				&& (recData.find("message") == string::npos))
			return recData;
		//printf("%s\n",recData.c_str());

	}
	return NULL;
}
bool redisSubscribeChannelClient::close_() 
{
	return m_redisSubscribeClient->closeClient();
}

bool redisSubscribeChannelClient::isconnect()
{
	return m_redisSubscribeClient->connectionCheck;
}

CRedisList::CRedisList() 
{
}
;
CRedisList::~CRedisList()
{
}

//by zx 20150415----------------------------------------------------------------
//bool CRedisList::connect(const string ip,int port)
bool CRedisList::connect(const string ip,int port,bool bEnableReconnect)
//------------------------------------------------------------------------------
{
	m_ip = ip;
	m_redisList = boost::shared_ptr<redis::client>(new redis::client(ip,port));
    if(m_redisList->connectionCheck==false)
    {
    	return false;
    }
	//by zx 20150415--------------------------------------------------------------
	m_redisList->SetReConnecteEnableFlag(bEnableReconnect);
	//----------------------------------------------------------------------------

	return true;
}
bool CRedisList::write(vector<data_redis> &writeDataVector,std::string strKeyprefix) {
	int writeLength = writeDataVector.size();
	std::vector<string> string_vector_field;
	std::vector<string> string_vector_value;
	for (int i = 0; i < writeLength; i++) {
		char key[255] = {0};
		char value[255] = "";
		char time[255] = "";
		char yearstr[255] = "";
		char monthstr[20] = "";
		char daystr[20] = "";
		sscanf(writeDataVector.at(i).time.c_str(), "%[^-]-%[^-]-%s", yearstr,
			monthstr, daystr);
		sprintf(time, "%s%s", yearstr, monthstr);
		sprintf(key, "%s%d#%d#%s",strKeyprefix.c_str(), writeDataVector.at(i).deviceID,
			writeDataVector.at(i).attriID, time);
		int leng=writeDataVector.at(i).time.length();
		if(writeDataVector.at(i).time.length()==16){
        //writeDataVector.at(i).time.erase(leng-3,3);
			writeDataVector.at(i).time+=":00";
		//writeDataVector.at(i).time[leng-1]=' ';
		//writeDataVector.at(i).time[leng-2]=' ';
		}
		sprintf(value, "%s,%d,%f,%f,%f", writeDataVector.at(i).time.c_str(),
			writeDataVector.at(i).status, writeDataVector.at(i).primaryData,
			writeDataVector.at(i).modifyData,
			writeDataVector.at(i).predictedData);

		int sendLength = m_redisList->rpush(key, value);
		if (sendLength <= 0) {
			return false;
		}

		int index = m_redisList->llen(key) - 1;
		if (index < -1) {
			return false;
		}
		char indexKey[255] = "";
		char indexValue[255] = "";
		sprintf(indexValue, "%d", index);
		sprintf(indexKey, "%sindex", key);
		string_vector_field.push_back(writeDataVector.at(i).time);
		string_vector_value.push_back(indexValue);

		if (m_redisList->hmset(indexKey, string_vector_field,
			string_vector_value) == false) {
				return false;
		}
		string_vector_field.clear();
		string_vector_value.clear();
	}
	return true;
}
bool CRedisList::readMultiData(const string key, vector<string> &data,
		int startIndex, int endIndex) {
	int receivelength = m_redisList->lrange(key, startIndex, endIndex, data);
	if (receivelength <= 0) {
		return false;
	}
	return true;
}
bool CRedisList::writeBYNAME(vector<data_redis> writeDataVector,std::string strKeyprefix)
{
	int writeLength = writeDataVector.size();
	std::vector<string> string_vector_field;
	std::vector<string> string_vector_value;
	for (int i = 0; i < writeLength; i++) {
		char key[255] = {0};
		char value[255] = "";
		char time[255] = "";
		char yearstr[255] = "";
		char monthstr[20] = "";
		char daystr[20] = "";
		
		sscanf(writeDataVector.at(i).time.c_str(),  "%[^-]-%[^-]-%s", yearstr,
			monthstr, daystr);
		int leng=writeDataVector.at(i).time.length();
		if(writeDataVector.at(i).time.length()==16){
			// writeDataVector.at(i).time.erase(leng-3,3);
		//writeDataVector.at(i).time[leng-1]='0';
		//writeDataVector.at(i).time[leng-2]='0';
			writeDataVector.at(i).time+=":00";
		}
		sprintf(time, "%s%s", yearstr, monthstr);
		sprintf(key, "%s%s#%s", strKeyprefix.c_str(),writeDataVector.at(i).name.c_str(), time);

		sprintf(value, "%d,%d,%s,%d,%f,%f,%f",
			writeDataVector.at(i).deviceID,
			writeDataVector.at(i).attriID,
			writeDataVector.at(i).time.c_str(),
			writeDataVector.at(i).status,
			writeDataVector.at(i).primaryData,
			writeDataVector.at(i).modifyData,
			writeDataVector.at(i).predictedData);

		int sendLength = m_redisList->rpush(key, value);
		if (sendLength <= 0) {
			return false;
		}

		int index = m_redisList->llen(key) - 1;
		if (index < -1) {
			return false;
		}
		char indexKey[255] = "";
		char indexValue[255] = "";
		sprintf(indexValue, "%d", index);
		sprintf(indexKey, "%sindex", key);
		string_vector_field.push_back(writeDataVector.at(i).time);
		string_vector_value.push_back(indexValue);

		if (m_redisList->hmset(indexKey, string_vector_field,
			string_vector_value) == false) {
				return false;
		}
		string_vector_field.clear();
		string_vector_value.clear();
	}
	return true;
}
bool CRedisList::readSingleData(const string key, string &data, int index) {
	data = m_redisList->lindex(key, index);
	if (data.size() == 0) {
		return false;
	}
	return true;
}

COperateData::COperateData() 
{
	; 
}

COperateData::~COperateData() {
}
COperateData* COperateData::m_COperateData = NULL;
bool COperateData::InitOperateData(const string ip,int port,int rate) {
	m_ip=ip;
	m_port=port;
	m_rate=rate;
	m_exception_data.push_back(-999);
	m_exception_data.push_back(-888);
	return true;
}
//by zx 20150415------------------------------------------------------
bool COperateData::Connect(bool bEnableReconnect) {
//--------------------------------------------------------------------
	return CRList.connect(m_ip,m_port,bEnableReconnect);
}
bool COperateData::PushData(vector<data_redis>& write_data_vector,std::string strKeyprefix) {
	return CRList.write(write_data_vector,strKeyprefix);
}
bool COperateData::GetData(int device_id, int attri_id, string start_time,
	string end_time, vector<data_redis>&data_vector) {
		int start_len=start_time.length();
		int end_len=start_time.length();

		if(((start_len==16)||(start_len==19))&&((end_len==16)||(end_len==19)))
		{

		}else
		{
			return false;
		}
		if (start_len==10)
		{
			start_time+=" 00:00";

		}
		if (start_len==19)
		{
			start_time[start_len-1]='0';
			start_time[start_len-2]='0';
			//start_time[start_len-3]=0;
		}
		if (end_len==10)
		{
			end_time+=" 00:00";

		}
		if (end_len==19)
		{
			end_time[end_len-1]='0';
			end_time[end_len-2]='0';
			//end_time[end_len-3]=0;
		}
		if (start_len==16)
		{
			start_time+=":00";
		}
		if (end_len==16)
		{
			end_time+=":00";
		}
		time_t start_time_long;
		time_t end_time_long;
		tm * last_start_time;
		tm* last_end_time;
		start_time+=":00";
		end_time+=":00";
		char index_key[50] = "";
		char start_key[50] = "";
		char get_vector_data[50] = "";
		char check[50] = "**nonexistent-key**";
		char checkConnect[50] = "connect failed";
		vector<string> index_vector;
		vector<string> field_vector;
		start_time_long=TimeFromString(start_time.c_str());
		tm * start_now_time=localtime(&start_time_long);
		sprintf(index_key, "%d#%d#%d%02dindex", device_id,attri_id, start_now_time->tm_year+1900,start_now_time->tm_mon+1);
		sprintf(start_key, "%d#%d#%d%02d", device_id,attri_id, start_now_time->tm_year+1900, start_now_time->tm_mon+1);
		field_vector.push_back(start_time);
		index_vector.clear();

		CRList.m_redisList->hmget(index_key, field_vector, index_vector);
		int receive_data_length = -1;
		sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
		if (strcmp(get_vector_data, check) == 0) {
			receive_data_length = 0;
		} else if (strcmp(get_vector_data, checkConnect) == 0) {
			return false;
		}
		if (receive_data_length == 0) {
			index_vector.clear();
			field_vector.clear();

			time_t time_long=TimeFromString(start_time.c_str());
			for (int i = 0;; i++) {
				time_long=time_long+60;
				start_time=timestamp(&time_long);
				tm*now_time=localtime(&time_long);
				last_start_time=now_time;
				sprintf(index_key, "%d#%d#%d%02dindex", device_id,attri_id, now_time->tm_year+1900,
					now_time->tm_mon+1);

				char time[50] = "";
				//sprintf(time, "%s-%02d-%02d %02d:%02d:%02d", now_time->tm_year, now_time->tm_mon,
				//	now_time->tm_mday, now_time->tm_hour, now_time->tm_min,now_time->tm_sec);
				sprintf(time, "%d-%02d-%02d %02d:%02d", now_time->tm_year+1900, now_time->tm_mon+1,
					now_time->tm_mday, now_time->tm_hour, now_time->tm_min);
				field_vector.clear();
				field_vector.push_back(time);

				sprintf(index_key, "%d#%d#%d%02dindex", device_id,attri_id,  now_time->tm_year+1900, now_time->tm_mon+1);
				CRList.m_redisList->hmget(index_key, field_vector, index_vector);
				sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
				if (strcmp(get_vector_data, check) == 0) {
					index_vector.clear();
					receive_data_length = 0;
				} else if (strcmp(get_vector_data, checkConnect) == 0) {
					return false;
				} else {
					receive_data_length = 1;
				}
				if (receive_data_length != 0) {
					sprintf(start_key, "%d#%d#%d%d", device_id,attri_id, now_time->tm_year+1900,
						now_time->tm_mon+1);
					break;
				}
				end_time_long=TimeFromString(end_time.c_str());
				if (time_long>end_time_long)
				{
					return true;
				}
			}
		}
		int year=start_now_time->tm_year;
		int mon=start_now_time->tm_mon;
		//int day=start_now_time->tm_mday;
		//int hour=start_now_time->tm_hour;
		//int minute=start_now_time->tm_min;
		int startPosition = atoi(index_vector.at(0).c_str());
		field_vector.clear();
		index_vector.clear();
		index_vector.clear();
		tm * end_now_time=localtime(&end_time_long);
		sprintf(index_key, "%d#%d#%d%02dindex", device_id,attri_id, end_now_time->tm_year+1900, end_now_time->tm_mon+1);
		field_vector.push_back(end_time);

		CRList.m_redisList->hmget(index_key, field_vector, index_vector);
		receive_data_length = index_vector.size();
		printf("%s", index_vector.at(0).c_str());

		sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
		if (strcmp(get_vector_data, check) == 0) {
			receive_data_length = 0;
		} else if (strcmp(get_vector_data, checkConnect) == 0) {
			return false;
		}
		if (receive_data_length == 0) {
			index_vector.clear();
			field_vector.clear();
			time_t time_long=end_time_long;
			for (int i = 0;; i++) {
				time_long=time_long-60;
				end_time=timestamp(&time_long);
				tm*now_time=localtime(&time_long);
				last_end_time=now_time;
				sprintf(index_key, "%d#%d#%d%dindex", device_id,attri_id, now_time->tm_year+1900,now_time->tm_mon+1);

				char time[50] = "";
				sprintf(time, "%d-%02d-%02d %02d:%02d", now_time->tm_year+1900, now_time->tm_mon+1, now_time->tm_mday,
					now_time->tm_hour, now_time->tm_min);
				field_vector.clear();
				field_vector.push_back(time);
				sprintf(index_key, "%d#%d#%d%dindex", device_id,attri_id, now_time->tm_year+1900,
					now_time->tm_mon+1);
				CRList.m_redisList->hmget(index_key, field_vector, index_vector);
				sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
				if (strcmp(get_vector_data, check) == 0) {
					index_vector.clear();
					receive_data_length = 0;
				} else if (strcmp(get_vector_data, checkConnect) == 0) {
					return false;
				} else {
					receive_data_length = 1;
				}
				if (receive_data_length != 0) {
					break;
				}
			}
		}
		int end_position = atoi(index_vector.at(0).c_str());

		//int endPosition=m_redisList->llen(startKey)-1;
		vector<string> out_vector;
		int year_count = last_end_time->tm_year -year;
		if (last_end_time->tm_year !=year) {

			for (int i =mon+1; i <= 12; i++) {

				if (i ==mon+1) {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen >= 0) {
						int readLen = CRList.m_redisList->lrange(start_key, startPosition,
							dayLen, out_vector);
						if (readLen == -1) {
							return false;
						}
					}
				} else {
					sprintf(start_key, "%d#%d#%d%02d", device_id,attri_id, last_start_time->tm_year+1900,
						i);
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen >= 0) {
						int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
							out_vector);
						if (readLen == -1) {
							return false;
						}
					}
				}

			}
			//开始的一年到最后一年中间的年份处理
			for (int i = 0; i < year_count - 1; i++) {
				for (int n = 0; n < 12; n++) {
					sprintf(start_key, "%d#%d#%d%d", device_id,attri_id,
						year+1900 + i + 1, n);
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen >= 0) {
						int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
							out_vector);
						if (readLen == -1) {
							return false;
						}
					}
				}
			}
			//最后一年

			for (int i = 1; i <= (last_end_time->tm_mon+1); i++) {
				sprintf(start_key, "%d#%d#%d%02d", device_id,attri_id, last_end_time->tm_year+1900, i);
				if (i == last_end_time->tm_mon+1) {
					CRList.m_redisList->lrange(start_key, 0, end_position, out_vector);
				} else {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen >= 0) {
						int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
							out_vector);
						if (readLen == -1) {
							return false;
						}
					}
				}

			}
		} else if ((mon+1) != (last_end_time->tm_mon+1)) {
			for (int i = mon+1; i <= (last_end_time->tm_mon+1); i++) {
				sprintf(start_key, "%d#%d#%d%02d",  device_id,attri_id, last_end_time->tm_year+1900, i);
				if (i == mon+1) {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen >= 0) {
						int readLen = CRList.m_redisList->lrange(start_key, startPosition,
							dayLen, out_vector);
						if (readLen == -1)
							return false;
					}

				} else if (i == (last_end_time->tm_mon+1)) {
					//int dayLen = m_redisList->llen(startKey) - 1;
					int readLen = CRList.m_redisList->lrange(start_key, 0, end_position,
						out_vector);
					if (readLen == -1)
						return false;

				} else {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen >= 0) {
						int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
							out_vector);
						if (readLen == -1)
							return false;
					}
				}
			}
		} else {

			int readLen = CRList.m_redisList->lrange(start_key, startPosition, end_position,
				out_vector);
			if (readLen == -1)
				return false;
		}
		int receive_length = out_vector.size();
		data_redis get_data;
		for (int i = 0; i < receive_length; i++) {
			char primary_data[50] = "";
			char modify_data[50] = "";
			char predicted_data[50] = "";
			char device_id_str[50]={50};
			char attri_id_str[50]={50};
			//sscanf(startTime.c_str(), "%[^-]-%[^-]-%s", start_year_str, start_month_str,start_day_str);
			char time[50] = "";
			sscanf(out_vector.at(i).c_str(), "%[^,],%[^,],%[^,],%[^,],%[^,],%s",device_id_str, attri_id_str,time,
				primary_data, modify_data, predicted_data);
			get_data.attriID = atoi(attri_id_str);
			get_data.deviceID = atoi(device_id_str);
			get_data.time = time;
			double val=atof(primary_data);
			get_data.primaryData = *((float*) &(val));
			val= atof(modify_data);
			get_data.modifyData = *((float*) &(val));
			val=atof(predicted_data);
			get_data.predictedData =  *((float*) &(val));
			data_vector.push_back(get_data);
		}
		//m_redisList->hmget()
		return true;
}

bool COperateData::ModifyData(int deviceID, int attriID,
	vector<data_redis> modify_vector_data) {
		vector<string> modify_data;
		char check[50] = "**nonexistent-key**";
		char check_connect[50] = "connect failed";
		char get_vector_data[50] = "";
		int modifyDataLen = modify_vector_data.size();
		for (int i = 0; i < modifyDataLen; i++) {
			char MData[50];
			sprintf(MData, "%s,%d,%f,%f,%f", modify_vector_data.at(i).time.c_str(),
				modify_vector_data.at(i).status, modify_vector_data.at(i).primaryData,
				modify_vector_data.at(i).modifyData, modify_vector_data.at(i).predictedData);
			modify_data.push_back(MData);
		}
		char start_time[50] = "";
		sprintf(start_time, "%s", modify_vector_data.at(0).time.c_str());
		int length = modify_vector_data.size();
		char end_time[50] = "";
		sprintf(end_time, "%s", modify_vector_data.at(length - 1).time.c_str());
		//endTime= modifyData.at(length - 1).time;

		int start_year = 0;
		int start_month = 0;
		//int startDay = 0;
		int end_year = 0;
		int end_month = 0;
		//int endDay = 0;
		char start_year_str[50] = "";
		char start_month_str[50] = "";
		char start_day_str[50] = "";
		char end_year_str[50] = "";
		char end_month_str[50] = "";
		char end_day_str[50] = "";
		char index_key[50] = "";
		char start_key[50] = "";
		sscanf(start_time, "%[^-]-%[^-]-%s", start_year_str, start_month_str,
			start_day_str);
		sscanf(end_time, "%[^-]-%[^-]-%s", end_year_str, end_month_str, end_day_str);
		start_year = atoi(start_year_str);
		start_month = atoi(start_month_str);
		//startDay=atoi(startDaystr);
		end_year = atoi(end_year_str);
		end_month = atoi(end_month_str);
		//endDay=atoi(endDaystr);
		vector<string> index_vector;
		vector<string> field_vector;

		sprintf(index_key, "%d#%d#%d%dindex", deviceID, attriID, start_year,
			start_month);
		sprintf(start_key, "%d#%d#%d%d", deviceID, attriID, start_year, start_month);
		field_vector.push_back(start_time);
		int yearCount = end_year - start_year;
		CRList.m_redisList->hmget(index_key, field_vector, index_vector);

		//int receiveDatalength = -1;
		sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
		if ((strcmp(get_vector_data, check_connect) == 0)
			|| (strcmp(get_vector_data, check) == 0)) {
				return false;
		}
		int startPosition = atoi(index_vector.at(0).c_str());

		field_vector.clear();
		index_vector.clear();
		sprintf(index_key, "%d#%d#%d%dindex", deviceID, attriID, end_year, end_month);
		field_vector.push_back(end_time);
		CRList.m_redisList->hmget(index_key, field_vector, index_vector);
		sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
		if ((strcmp(get_vector_data, check_connect) == 0)
			|| (strcmp(get_vector_data, check) == 0)) {
				return false;
		}
		int endPosition = atoi(index_vector.at(0).c_str());
		//int endPosition=m_redisList->llen(startKey)-1;
		vector<string> out_vector;
		if (start_year != end_year) {
			int modifyCount = 0;
			for (int i = start_month; i <= 12; i++) {

				if (i == start_month) {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					for (int n = startPosition; n <= dayLen; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}
				} else {
					sprintf(start_key, "%d#%d#%d%d", deviceID, attriID, start_year,
						i);
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					for (int n = 0; n <= dayLen; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}
				}

			}
			for (int i = 0; i < yearCount - 1; i++) {
				for (int n = 0; n < 12; n++) {
					sprintf(start_key, "%d#%d#%d%d", deviceID, attriID,
						start_year + i + 1, n);
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					if (dayLen < -1) {
						return false;
					}
					if (CRList.m_redisList->lrange(start_key, 0, dayLen, out_vector)
						== -1) {
							return false;
					}
					for (int n = 0; n <= dayLen; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}
				}
			}
			for (int i = 1; i <= end_month; i++) {
				sprintf(start_key, "%d#%d#%d%d", deviceID, attriID, end_year, i);
				if (i == end_month) {
					for (int n = 0; n <= endPosition; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}
				} else {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					//m_redisList->lrange(startKey, 0, dayLen, out_vector);
					for (int n = 0; n <= dayLen - 1; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}
				}

			}
		} else if (start_month != end_month) {
			int modifyCount = 0;
			for (int i = start_month; i <= end_month; i++) {
				sprintf(start_key, "%d#%d#%d%d", deviceID, attriID, end_year, i);
				if (i == start_month) {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					//m_redisList->lrange(startKey, startPosition, dayLen,out_vector);
					for (int n = startPosition; n <= dayLen; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}

				} else if (i == end_month) {
					//int dayLen = m_redisList->llen(startKey) - 1;
					//m_redisList->lrange(startKey, 0, endPosition, out_vector);
					for (int n = 0; n <= endPosition; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}

				} else {
					int dayLen = CRList.m_redisList->llen(start_key) - 1;
					for (int n = 0; n <= dayLen; n++) {
						if (CRList.m_redisList->lset(start_key, n,
							modify_data.at(modifyCount)) == false) {
								return false;
						}
						modifyCount++;
					}
				}
			}
		} else {
			int modifyCount = 0;
			for (int n = startPosition; n <= endPosition; n++) {
				if (CRList.m_redisList->lset(start_key, n, modify_data.at(modifyCount))
					== false) {
						return false;
				}
				modifyCount++;
			}
		}
		return true;
}

bool COperateData::IsExist(int device_id, int attri_id, string time, data_redis &point) {
	vector<data_redis> data_vector;
	if (GetData(device_id, attri_id, time, time, data_vector) == false) {
		return false;
	}
	int size = data_vector.size();
	if (size == 0) {
		return false;
	} else {
		point = data_vector.at(0);
		return true;
	}

}
bool COperateData::GetLastTimeData(int device_id, int attri_id, string time,
	data_redis &point) {
		int start_year = 0;
		int start_month = 0;
		int start_day = 0;
		int start_hour = 0;
		int start_minute = 0;
		char start_year_str[50] = "";
		char start_month_str[50] = "";
		char start_day_str[50] = "";
		char start_hour_str[50] = "";
		char start_minute_str[50] = "";

		char indexKey[50] = "";
		char startKey[50] = "";
		char getVectorData[50] = "";
		char check[50] = "**nonexistent-key**";
		char checkConnect[50] = "connect failed";
		sscanf(time.c_str(), "%[^-]-%[^-]-%[^ ] %[^:]:%s", start_year_str,
			start_month_str, start_day_str, start_hour_str, start_minute_str);
		start_year = atoi(start_year_str);
		start_month = atoi(start_month_str);
		start_day = atoi(start_day_str);
		start_hour = atoi(start_hour_str);
		start_minute = atoi(start_minute_str);
		vector<string> index_vector;
		vector<string> field_vector;

		sprintf(indexKey, "%d#%d#%d%dindex", device_id, attri_id, start_year,
			start_month);
		sprintf(startKey, "%d#%d#%d%d", device_id, attri_id, start_year, start_month);
		field_vector.push_back(time);
		index_vector.clear();

		CRList.m_redisList->hmget(indexKey, field_vector, index_vector);
		int receiveDatalength = -1;
		sprintf(getVectorData, "%s", index_vector.at(0).c_str());
		if (strcmp(getVectorData, check) == 0) {
			receiveDatalength = 0;
		} else if (strcmp(getVectorData, checkConnect) == 0) {
			return false;
		}
		if (receiveDatalength == 0) {
			index_vector.clear();
			field_vector.clear();
			for (int i = 0; i < m_rate; i++) {
				if (start_minute + 1 >= 60) {
					start_minute = start_minute + 1 - 60;
					start_hour = start_hour + 1;
					if (start_hour >= 24) {
						start_hour = start_hour - 24;
						start_day = start_day + 1;
						if (start_month != 2) {
							if ((start_month == 1) || (start_month == 3)
								|| (start_month == 5) || (start_month == 7)
								|| (start_month == 8) || (start_month == 10)
								|| (start_month == 12)) {
									if (start_day > 31) {
										start_month = start_month + 1;
										start_day = 1;
									}
							} else {
								if (start_day > 30) {
									start_month = start_month + 1;
									start_day = 1;
								}
							}
							if (start_month == 13) {
								start_month = 1;
								start_year = start_year + 1;
							}
						} else {
							if ((start_year % 4) == 0) {
								if (start_day > 29) {
									start_month = start_month + 1;
									start_day = 1;
								}
							} else {
								if (start_day > 28) {
									start_month = start_month + 1;
									start_day = 1;
								}
							}
						}
					}
				} else {
					start_minute = start_minute + 1;

				}
				sprintf(indexKey, "%d#%d#%d%dindex", device_id, attri_id, start_year,
					start_month);
				sprintf(start_year_str, "%d", start_year);
				if (start_month < 10) {
					sprintf(start_month_str, "0%d", start_month);
				} else {
					sprintf(start_month_str, "%d", start_month);
				}
				if (start_day < 10) {
					sprintf(start_day_str, "0%d", start_day);
				} else {
					sprintf(start_day_str, "%d", start_day);
				}
				if (start_hour < 10) {
					sprintf(start_hour_str, "0%d", start_hour);
				} else {
					sprintf(start_hour_str, "%d", start_hour);
				}
				if (start_minute < 10) {
					sprintf(start_minute_str, "0%d", start_minute);
				} else {
					sprintf(start_minute_str, "%d", start_minute);
				}
				char time[50] = "";
				sprintf(time, "%s-%s-%s %s:%s", start_year_str, start_month_str,
					start_day_str, start_hour_str, start_minute_str);
				field_vector.clear();
				field_vector.push_back(time);

				sprintf(indexKey, "%d#%d#%d%dindex", device_id, attri_id, start_year,
					start_month);
				CRList.m_redisList->hmget(indexKey, field_vector, index_vector);
				sprintf(getVectorData, "%s", index_vector.at(0).c_str());
				if (strcmp(getVectorData, check) == 0) {
					index_vector.clear();
					receiveDatalength = 0;
				} else if (strcmp(getVectorData, checkConnect) == 0) {
					return false;
				} else {
					receiveDatalength = 1;
				}
				if (receiveDatalength != 0) {
					sprintf(startKey, "%d#%d#%d%d", device_id, attri_id, start_year,
						start_month);
					int startPosition = atoi(index_vector.at(0).c_str());
					string data_string;
					data_string = CRList.m_redisList->lindex(startKey, startPosition);
					if ((strcmp(data_string.c_str(), check) == 0)
						||(strcmp(data_string.c_str(), checkConnect) == 0)) {
						return false;
					} else {
						char primaryData[50] = "";
						char modifyData[50] = "";
						char predictedData[50] = "";
						char time[50] = "";
						sscanf(data_string.c_str(), "%[^,],%[^,],%[^,],%s", time,
							primaryData, modifyData, predictedData);
						point.deviceID = device_id;
						point.attriID = attri_id;
						point.time = time;
						
						double val= atof(modifyData);
						point.modifyData = *((float*) &(val));
						val=atof(predictedData);
						point.predictedData =  *((float*) &(val));
						val=atof(primaryData);
						point.primaryData= *((float*) &(val));
						return true;
					}
				}
			}
		}
		return true;
}
bool COperateData::GetData(string name, string start_time, string end_time,
	vector<data_redis>&data_vector)
{
	int start_len=start_time.length();
	int end_len=start_time.length();
	
	if(((start_len==16)||(start_len==19))&&((end_len==16)||(end_len==19)))
	{
		
	}else
	{
		return false;
	}
	if (start_len==10)
	{
		start_time+=" 00:00";

	}
	if (start_len==19)
	{
		start_time[start_len-1]='0';
		start_time[start_len-2]='0';
		//start_time[start_len-3]=0;
	}
	if (end_len==10)
	{
		end_time+=" 00:00";

	}
	if (end_len==19)
	{
		end_time[end_len-1]='0';
		end_time[end_len-2]='0';
		//end_time[end_len-3]=0;
	}
	if (start_len==16)
	{
		start_time+=":00";
	}
	if (end_len==16)
	{
		end_time+=":00";
	}
	time_t start_time_long;
	time_t end_time_long;
	//tm * last_start_time;
	tm* last_end_time;
	end_time_long=TimeFromString(end_time.c_str());
    char index_key[50] = "";
	char start_key[50] = "";
	char get_vector_data[1024] = "";
	char check[50] = "**nonexistent-key**";
	char checkConnect[50] = "connect failed";
	vector<string> index_vector;
	vector<string> field_vector;
	start_time_long=TimeFromString(start_time.c_str());
	tm * start_now_time=localtime(&start_time_long);
	char time1[50] = "";
	//sprintf(time, "%s-%02d-%02d %02d:%02d:%02d", now_time->tm_year, now_time->tm_mon,
	//	now_time->tm_mday, now_time->tm_hour, now_time->tm_min,now_time->tm_sec);
	sprintf(time1, "%d-%02d-%02d %02d:%02d:%02d", start_now_time->tm_year+1900, start_now_time->tm_mon+1,
		start_now_time->tm_mday, start_now_time->tm_hour, start_now_time->tm_min,start_now_time->tm_sec);
	sprintf(index_key, "%s#%d%02dindex", name.c_str(), start_now_time->tm_year+1900,start_now_time->tm_mon+1);
	sprintf(start_key, "%s#%d%02d",name.c_str(), start_now_time->tm_year+1900, start_now_time->tm_mon+1);
	//last_start_time=localtime(&start_time_long);
	last_end_time=localtime(&end_time_long);
	tm * end_now_time=localtime(&end_time_long);
	
	
	field_vector.push_back(time1);
	index_vector.clear();

	CRList.m_redisList->hmget(index_key, field_vector, index_vector);
    int receive_data_length = -1;
	sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
	if (strcmp(get_vector_data, check) == 0) {
		receive_data_length = 0;
	} else if (strcmp(get_vector_data, checkConnect) == 0) {
		return false;
	}
	time_t time_long=TimeFromString(start_time.c_str());
	if (receive_data_length == 0) {
		index_vector.clear();
		field_vector.clear();
		
		
		
		for (int i = 0;; i++) {
			time_long=time_long+60;
			start_time=timestamp(&time_long);
			tm*now_time=localtime(&time_long);
			
			sprintf(index_key, "%s#%d%02dindex", name.c_str(), now_time->tm_year+1900,
				now_time->tm_mon+1);
			
			char time[50] = "";
			//sprintf(time, "%s-%02d-%02d %02d:%02d:%02d", now_time->tm_year, now_time->tm_mon,
			//	now_time->tm_mday, now_time->tm_hour, now_time->tm_min,now_time->tm_sec);
			sprintf(time, "%d-%02d-%02d %02d:%02d:%02d", now_time->tm_year+1900, now_time->tm_mon+1,
				now_time->tm_mday, now_time->tm_hour, now_time->tm_min,now_time->tm_sec);
			field_vector.clear();
			field_vector.push_back(time);

			sprintf(index_key, "%s#%d%02dindex", name.c_str(),  now_time->tm_year+1900, now_time->tm_mon+1);
			CRList.m_redisList->hmget(index_key, field_vector, index_vector);
			sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
			if (strcmp(get_vector_data, check) == 0) {
				index_vector.clear();
				receive_data_length = 0;
			} else if (strcmp(get_vector_data, checkConnect) == 0) {
				return false;
			} else {
				receive_data_length = 1;
			}
			if (receive_data_length != 0) {
				sprintf(start_key, "%s#%d%02d", name.c_str(), now_time->tm_year+1900,
					now_time->tm_mon+1);
				break;
			}
		
		if (time_long>end_time_long)
		{
			return true;
		}
		}
	}

	int startPosition = atoi(index_vector.at(0).c_str());
    field_vector.clear();
	index_vector.clear();
	index_vector.clear();
	int year=start_now_time->tm_year;
	int mon=start_now_time->tm_mon;
	//int day=start_now_time->tm_mday;
	//int hour=start_now_time->tm_hour;
	//int minute=start_now_time->tm_min;
	sprintf(index_key, "%s#%d%02dindex",name.c_str(), end_now_time->tm_year+1900, end_now_time->tm_mon+1);
	field_vector.push_back(end_time);

	CRList.m_redisList->hmget(index_key, field_vector, index_vector);
	receive_data_length = index_vector.size();
	printf("%s", index_vector.at(0).c_str());

	sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
	if (strcmp(get_vector_data, check) == 0) {
		receive_data_length = 0;
	} else if (strcmp(get_vector_data, checkConnect) == 0) {
		return false;
	}
	
	if (receive_data_length == 0) {
		index_vector.clear();
		field_vector.clear();
		time_t time_long=end_time_long;
		
		for (int i = 0;; i++) {
			time_long=time_long-60;
			end_time=timestamp(&time_long);
			tm*now_time1=localtime(&time_long);
			last_end_time=now_time1;
			sprintf(index_key, "%s#%d%dindex", name.c_str(), now_time1->tm_year+1900,now_time1->tm_mon+1);
			
			char time[50] = "";
			sprintf(time, "%d-%02d-%02d %02d:%02d:%02d", now_time1->tm_year+1900, now_time1->tm_mon+1, now_time1->tm_mday,
				now_time1->tm_hour, now_time1->tm_min,now_time1->tm_sec);
			field_vector.clear();
			field_vector.push_back(time);
			sprintf(index_key, "%s#%d%02dindex", name.c_str(), now_time1->tm_year+1900,
				now_time1->tm_mon+1);
			CRList.m_redisList->hmget(index_key, field_vector, index_vector);
			sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
			if (strcmp(get_vector_data, check) == 0) {
				index_vector.clear();
				receive_data_length = 0;
			} else if (strcmp(get_vector_data, checkConnect) == 0) {
				return false;
			} else {
				receive_data_length = 1;
			}
			if (receive_data_length != 0) {
				break;
			}
		}
	}
	int end_position = atoi(index_vector.at(0).c_str());
	//last_start_time=localtime(&time_long);
	//int endPosition=m_redisList->llen(startKey)-1;
	vector<string> out_vector;
	int year_count = last_end_time->tm_year - year;
	if (last_end_time->tm_year !=year) {

		for (int i = mon+1; i <= 12; i++) {

			if (i == mon+1) {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen >= 0) {
					int readLen = CRList.m_redisList->lrange(start_key, startPosition,
						dayLen, out_vector);
					if (readLen == -1) {
						return false;
					}
				}
			} else {
				sprintf(start_key, "%s#%d%02d", name.c_str(),year+1900,
					i);
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen >= 0) {
					int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
						out_vector);
					if (readLen == -1) {
						return false;
					}
				}
			}

		}
		//开始的一年到最后一年中间的年份处理
		for (int i = 0; i < year_count - 1; i++) {
			for (int n = 0; n < 12; n++) {
				sprintf(start_key, "%s#%d%d", name.c_str(),
					year+1900 + i + 1, n);
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen >= 0) {
					int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
						out_vector);
					if (readLen == -1) {
						return false;
					}
				}
			}
		}
		//最后一年

		for (int i = 1; i <= (last_end_time->tm_mon+1); i++) {
			sprintf(start_key, "%s#%d%02d", name.c_str(), last_end_time->tm_year+1900, i);
			if (i == last_end_time->tm_mon+1) {
				CRList.m_redisList->lrange(start_key, 0, end_position, out_vector);
			} else {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen >= 0) {
					int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
						out_vector);
					if (readLen == -1) {
						return false;
					}
				}
			}

		}
	} else if ((mon+1) != (last_end_time->tm_mon+1)) {
		for (int i =mon+1; i <= (last_end_time->tm_mon+1); i++) {
			sprintf(start_key, "%s#%d%02d", name.c_str(), last_end_time->tm_year+1900, i);
			if (i == mon+1) {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen >= 0) {
					int readLen = CRList.m_redisList->lrange(start_key, startPosition,
						dayLen, out_vector);
					if (readLen == -1)
						return false;
				}

			} else if (i == (last_end_time->tm_mon+1)) {
				//int dayLen = m_redisList->llen(startKey) - 1;
				int readLen = CRList.m_redisList->lrange(start_key, 0, end_position,
					out_vector);
				if (readLen == -1)
					return false;

			} else {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen >= 0) {
					int readLen = CRList.m_redisList->lrange(start_key, 0, dayLen,
						out_vector);
					if (readLen == -1)
						return false;
				}
			}
		}
	} else {

		int readLen = CRList.m_redisList->lrange(start_key, startPosition, end_position,
			out_vector);
		if (readLen == -1)
			return false;
	}
	int receive_length = out_vector.size();
	data_redis get_data;
	for (int i = 0; i < receive_length; i++) {
		char primary_data[50] = "";
		char modify_data[50] = "";
		char predicted_data[50] = "";
		char device_id_str[50]={0};
		char attri_id_str[50]={0};
		char status_data[50]={0};
		//sscanf(startTime.c_str(), "%[^-]-%[^-]-%s", start_year_str, start_month_str,start_day_str);
		char time[50] = "";
		sscanf(out_vector.at(i).c_str(), "%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%s",device_id_str, attri_id_str,time,status_data,
			primary_data, modify_data, predicted_data);
		get_data.status=atoi(status_data);
		get_data.attriID = atoi(attri_id_str);
		get_data.deviceID = atoi(device_id_str);
		get_data.time = time;
		double val=atof(primary_data);//*((float*) &(val))
		get_data.primaryData = val;
		val= atof(modify_data);
		get_data.modifyData =val;
		val=atof(predicted_data);
		get_data.predictedData =val;
		get_data.name=name;
		data_vector.push_back(get_data);
	}
	//m_redisList->hmget()
	return true;
}
bool COperateData::ModifyData(string name, vector<data_redis> modify_vector_data)
{
	vector<string> modify_data;
	char check[50] = "**nonexistent-key**";
	char check_connect[50] = "connect failed";
	char get_vector_data[50] = "";
	int modifyDataLen = modify_vector_data.size();
	for (int i = 0; i < modifyDataLen; i++) {
		char MData[50];
		sprintf(MData, "%s,%d,%f,%f,%f", modify_vector_data.at(i).time.c_str(),
			modify_vector_data.at(i).status, modify_vector_data.at(i).primaryData,
			modify_vector_data.at(i).modifyData, modify_vector_data.at(i).predictedData);
		modify_data.push_back(MData);
	}
	char startTime[50] = "";
	sprintf(startTime, "%s", modify_vector_data.at(0).time.c_str());
	int length = modify_vector_data.size();
	char endTime[50] = "";
	sprintf(endTime, "%s", modify_vector_data.at(length - 1).time.c_str());
	//endTime= modifyData.at(length - 1).time;

	int start_year = 0;
	int start_month = 0;
	//int startDay = 0;
	int end_year = 0;
	int end_month = 0;
	//int endDay = 0;
	char start_year_str[50] = "";
	char start_month_str[50] = "";
	char start_day_str[50] = "";
	char end_year_str[50] = "";
	char end_month_str[50] = "";
	char end_day_str[50] = "";
	char index_key[50] = "";
	char start_key[50] = "";
	sscanf(startTime, "%[^-]-%[^-]-%s", start_year_str, start_month_str,
		start_day_str);
	sscanf(endTime, "%[^-]-%[^-]-%s", end_year_str, end_month_str, end_day_str);
	start_year = atoi(start_year_str);
	start_month = atoi(start_month_str);
	//startDay=atoi(startDaystr);
	end_year = atoi(end_year_str);
	end_month = atoi(end_month_str);
	//endDay=atoi(endDaystr);
	vector<string> index_vector;
	vector<string> field_vector;

	sprintf(index_key, "%s#%d%dindex", name.c_str(), start_year,
		start_month);
	sprintf(start_key, "%s#%d%d", name.c_str(), start_year, start_month);
	field_vector.push_back(startTime);
	int yearCount = end_year - start_year;
	CRList.m_redisList->hmget(index_key, field_vector, index_vector);

	//int receiveDatalength = -1;
	sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
	if ((strcmp(get_vector_data, check_connect) == 0)
		|| (strcmp(get_vector_data, check) == 0)) {
			return false;
	}
	int startPosition = atoi(index_vector.at(0).c_str());

	field_vector.clear();
	index_vector.clear();
	sprintf(index_key, "%s#%d%dindex", name.c_str(), end_year, end_month);
	field_vector.push_back(endTime);
	CRList.m_redisList->hmget(index_key, field_vector, index_vector);
	sprintf(get_vector_data, "%s", index_vector.at(0).c_str());
	if ((strcmp(get_vector_data, check_connect) == 0)
		|| (strcmp(get_vector_data, check) == 0)) {
			return false;
	}
	int end_position = atoi(index_vector.at(0).c_str());
	//int endPosition=m_redisList->llen(startKey)-1;
	vector<string> out_vector;
	if (start_year != end_year) {
		int modifyCount = 0;
		for (int i = start_month; i <= 12; i++) {

			if (i == start_month) {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				for (int n = startPosition; n <= dayLen; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}
			} else {
				sprintf(start_key, "%s#%d%d", name.c_str(), start_year,
					i);
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				for (int n = 0; n <= dayLen; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}
			}

		}
		for (int i = 0; i < yearCount - 1; i++) {
			for (int n = 0; n < 12; n++) {
				sprintf(start_key, "%s#%d%02d", name.c_str(),
					start_year + i + 1, n);
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				if (dayLen < -1) {
					return false;
				}
				if (CRList.m_redisList->lrange(start_key, 0, dayLen, out_vector)
					== -1) {
						return false;
				}
				for (int n = 0; n <= dayLen; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}
			}
		}
		for (int i = 1; i <= end_month; i++) {
			sprintf(start_key, "%s#%d%02d", name.c_str(), end_year, i);
			if (i == end_month) {
				for (int n = 0; n <= end_position; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}
			} else {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				//m_redisList->lrange(startKey, 0, dayLen, out_vector);
				for (int n = 0; n <= dayLen - 1; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}
			}

		}
	} else if (start_month != end_month) {
		int modifyCount = 0;
		for (int i = start_month; i <= end_month; i++) {
			sprintf(start_key, "%s#%d%d", name.c_str(), end_year, i);
			if (i == start_month) {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				//m_redisList->lrange(startKey, startPosition, dayLen,out_vector);
				for (int n = startPosition; n <= dayLen; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}

			} else if (i == end_month) {
				//int dayLen = m_redisList->llen(startKey) - 1;
				//m_redisList->lrange(startKey, 0, endPosition, out_vector);
				for (int n = 0; n <= end_position; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}

			} else {
				int dayLen = CRList.m_redisList->llen(start_key) - 1;
				for (int n = 0; n <= dayLen; n++) {
					if (CRList.m_redisList->lset(start_key, n,
						modify_data.at(modifyCount)) == false) {
							return false;
					}
					modifyCount++;
				}
			}
		}
	} else {
		int modifyCount = 0;
		for (int n = startPosition; n <= end_position; n++) {
			if (CRList.m_redisList->lset(start_key, n, modify_data.at(modifyCount))
				== false) {
					return false;
			}
			modifyCount++;
		}
	}
	return true;
}
bool COperateData::PushDataBYNAME(vector<data_redis> &write_data_vector,std::string strKeyprefix)
{
	return CRList.writeBYNAME(write_data_vector,strKeyprefix);
}
bool COperateData::IsExist(string name, string time,
	data_redis &point) {
		if ((time.length()==16)||(time.length()==19))
		{
		}else
		{
	       return false;
		}
		if (time.length()==16)
		{
			time+=":00";
		}
		time_t time_long=TimeFromString(time.c_str());
		time_long=time_long-m_rate*60;
		string last_time=timestamp(&time_long);
		vector<data_redis> data_vector;
		if (GetData(name, last_time,time, data_vector) == false) {
			return false;
		}
		int size = data_vector.size();
		if (size == 0) {
			return false;
		} else{
			point = data_vector.at(size-1);
			return true;
		}

}
bool COperateData::GetLastTimeData(string name, string time,
	data_redis &point) {
		if ((time.length()==16)||(time.length()==19))
		{
		}else
		{
			return false;
		}
		if (time.length()==16)
		{
			time+=":00";
		}
		data_redis point_temp;
		if (IsExist(name,time,point_temp))
		{
			time_t time_long=TimeFromString(time.c_str())-m_rate*60;
			string now_time=timestamp(&time_long);
			return IsExist(name,now_time,point);
		}
		
		return false;
		// *((float*) &(val));
}
const char * COperateData::timestamp(time_t* t)
{
	static char buf[64];
	time_t tThen = t? *t:time(NULL);
	tm* pTM = localtime(&tThen);
	sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", pTM->tm_year+1900, pTM->tm_mon+1, pTM->tm_mday, pTM->tm_hour, pTM->tm_min,pTM->tm_sec);
	return buf;
}
//@func:时间由字符串(String)转化为秒数(time_t)
time_t COperateData::TimeFromString(const char* strTime)
{
	struct tm t={0};
	if (strTime==NULL) return 0;
	if (sscanf(strTime, "%d-%d-%d %d:%d:%d", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec)==6)
	{
		t.tm_year -= 1900;
		t.tm_mon -= 1;
		return mktime(&t);
	}
	memset(&t, 0, sizeof(t));
	if (sscanf(strTime, "%d-%d-%d", &t.tm_year, &t.tm_mon, &t.tm_mday)==3)
	{
		t.tm_year -= 1900;
		t.tm_mon -= 1;
		return mktime(&t);
	}
	return 0;
}

bool COperateData::GetDeviceName(string pattern,vector<string>&table_name)
{
	vector<string>key_name;
	if (CRList.GetAllKey(pattern,key_name))
	{
		char name[50]={0};
		char time[50]={0};
		string name_str="";
		int key_name_vector_len=key_name.size();
		for (int i=0;i<key_name_vector_len;i++)
		{
			sscanf(key_name.at(i).c_str(), "%[^#]#%s",name,time);
			name_str=name;
			vector<string>::iterator result=find(table_name.begin(),table_name.end(),name_str);
			if (result==table_name.end())
			{
				table_name.push_back(name_str);
			}

		}
	}else
	{
		return false;
	}
	return true;
}
bool COperateData::GetLastCorrectValue(string name,string time,double &val)
{
	if (time.length()==16)
	{
		time+=":00";
	}else if (time.length()==19)
	{
	}else
	{
		return false;
	}
	time_t time_long=TimeFromString(time.c_str());
	string start_time=timestamp(&time_long);
	string pattern;
	pattern=name;
	pattern+="#??????";
	vector<string>key_name;
	//CRList.GetAllKey(pattern,key_name);
	//if (key_name.size()==0)
	//{
	//	return false;
	//}
	/*string min_time;
	string min_key=key_name.at(0);
	int length=min_key.length();
	string year="0000";string month="00";
	year[0]=min_key[length-6];
	year[1]=min_key[length-5];
	year[2]=min_key[length-4];
	year[3]=min_key[length-3];
	month[0]=min_key[length-2];
	month[1]=min_key[length-1];
	min_time+=year+"-";
	min_time+=month+"-01 00:00:00";
	if(key_name.size()!=1)
	{
		int keylen=key_name.size();
		for (int i=0;i<keylen;i++)
		{
			string time_temp="0000000";
			time_temp[0]=key_name.at(i)[length-6];
			time_temp[1]=key_name.at(i)[length-5];
			time_temp[2]=key_name.at(i)[length-4];
			time_temp[3]=key_name.at(i)[length-3];
			time_temp[4]='-';
			time_temp[5]=key_name.at(i)[length-2];
			time_temp[6]=key_name.at(i)[length-1];
			time_temp+="-01 00:00:00 ";
			if (time_temp<min_time)
			{
				min_time=time_temp;
				min_key=key_name.at(i);
			}
		}
	}*/
	string single_data;
	//CRList.readSingleData(min_key,single_data,0);
	char devid[50]="";
	char attrid[50]="";
	char date[50]="";
	char value[100]="";
	sscanf(single_data.c_str(),"%[^,],%[^,],%[^,],%s",devid,attrid,date,value);
	time_t time_temp=TimeFromString(date);
	int index=0;
    //while(time_long>=time_temp){ 
		time_long=time_long-3600;
		string end_time=timestamp(&time_long);
		vector<data_redis>get_data_temp;
		if(GetData(name,end_time,start_time,get_data_temp))
		{
			
			int size=get_data_temp.size();
			if (size>0)
			{
				for (int i=size-1;i>=0;i--)
				{
					
						if (((get_data_temp.at(i).primaryData)!=(m_exception_data.at(0)))&&((get_data_temp.at(i).primaryData)!=(m_exception_data.at(1))))
						{
							val=(get_data_temp.at(i).primaryData);
							return true;
						}
					
				}
			}
		}else
		{
			return false;
		}
		//start_time=timestamp(&time_long);
    //}
	return true;
}
bool CRedisList::GetLastTimeforMonth(string key,string &time)
{
	int length=m_redisList->llen(key);
	if (length==-1)
	{
		return false;
	}
	char check[50]="connect failed";
	string val=m_redisList->lindex(key,length-1);
	if (strcmp(check,val.c_str())==0)
	{
		return false;
	}
	char val_char[100]={0};
	//sprintf(val_char,"%s",val);
	int len=val.length();
	for (int n=0;n<len;n++)
	{
		val_char[n]=val[n];
	}
	char devID[25]="";
	char attrID[25]="";
	char time_char[25]="";
	char value[100]="";
	sscanf(val_char,"%[^,],%[^,],%[^,],%s",devID,attrID,time_char,value);
	time_t time_long=TimeFromString(time_char);
	time=timestamp(&time_long);
	return true;
}
bool CRedisList::GetAllKey(string pattern,vector<string>&key_name)
{
	if(m_redisList->keys(pattern,key_name)==-1)
	{
		return false;
	}
	return true;
}
const char * CRedisList::timestamp(time_t* t)
{
	static char buf[64];
	time_t tThen = t? *t:time(NULL);
	tm* pTM = localtime(&tThen);
	sprintf(buf, "%04d-%02d-%02d %02d:%02d:%02d", pTM->tm_year+1900, pTM->tm_mon+1, pTM->tm_mday, pTM->tm_hour, pTM->tm_min,pTM->tm_sec);
	return buf;
}

time_t CRedisList::TimeFromString(const char * strTime)
{
	struct tm t={0};
	if (strTime==NULL) return 0;
	if (sscanf(strTime, "%d-%d-%d %d:%d:%d", &t.tm_year, &t.tm_mon, &t.tm_mday, &t.tm_hour, &t.tm_min, &t.tm_sec)==6)
	{
		t.tm_year -= 1900;
		t.tm_mon -= 1;
		return mktime(&t);
	}
	memset(&t, 0, sizeof(t));
	if (sscanf(strTime, "%d-%d-%d", &t.tm_year, &t.tm_mon, &t.tm_mday)==3)
	{
		t.tm_year -= 1900;
		t.tm_mon -= 1;
		return mktime(&t);
	}
	return 0;
}
bool COperateData::Push(string key,string val)
{
	
	return CRList.m_redisList->set(key,val);
}

bool COperateData::Get(string key,string &val)
{
	char check[50]="**nonexistent-key**";
	char checkConnect[50]="connect failed";
	val=CRList.m_redisList->get(key);
	if (strcmp(val.c_str(),check)==0)
	{
		val.clear();
		return false;
	}else if (strcmp(val.c_str(),checkConnect)==0)
	{
		val.clear();
		return false;
	}else
	{
        return true;
	}
}
bool COperateData::DeleteData(string key,int count)
{
	if (count==-1)
	{
		bool check=CRList.m_redisList->del(key);
		if (check==false)
		{
			return false;
		}
		//sprintf(indexValue, "%d", index);
		
		char str[50];
		sprintf(str, "%sindex", key.c_str());
		bool checkH=CRList.m_redisList->del(str);
		if (checkH==false)
		{
			return false;
		}
		return true;
	}else
	{
		char str[50];
		sprintf(str, "%sindex", key.c_str());
		bool checkH=CRList.m_redisList->del(str);
		if (checkH==false)
		{
			return false;
		}
		for (int i=0;i<count;i++)
		{
			string valstr=CRList.m_redisList->lpop(key);
			if (valstr.length()<=0)
			{
				break;
			}
		}
		int len=CRList.m_redisList->llen(key);
		if (len<=0)
		{
			return true;
		}
		vector<string> getVal;
		int getLen=CRList.m_redisList->lrange(key,0,len-1,getVal);
		if (getLen==-1)
		{
			return false;
		}
		int receive_length = getVal.size();

	    for (int i = 0; i < receive_length; i++) {
			char primary_data[50] = "";
			char modify_data[50] = "";
			char predicted_data[50] = "";
			char device_id_str[50]={0};
			char attri_id_str[50]={0};
			char status_data[50]={0};
			//sscanf(startTime.c_str(), "%[^-]-%[^-]-%s", start_year_str, start_month_str,start_day_str);
			char time[50] = "";
			sscanf(getVal.at(i).c_str(), "%[^,],%[^,],%[^,],%[^,],%[^,],%[^,],%s",device_id_str, attri_id_str,time,status_data,
				primary_data, modify_data, predicted_data);
			char index[50];
			sprintf(index, "%d", i);
			std::vector<string> string_vector_field;
			std::vector<string> string_vector_value;
			string_vector_field.push_back(time);
			string_vector_value.push_back(index);
			if (CRList.m_redisList->hmset(str,string_vector_field,string_vector_value) == false) {
					return false;
			}
		
	}
		return true;
	}
}
int COperateData::GetLenofKey(string key)
{
	return CRList.m_redisList->llen(key);
}

bool COperateData::PushMultiData(const vector<pair<string, string>>& mdata)
{
	CRList.m_redisList->mset(mdata);
	return true;
}

