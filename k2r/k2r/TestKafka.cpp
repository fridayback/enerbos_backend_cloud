#if 1
#include "kafka_consumer.h"
#include "kafka_producer.h"
#include "comm/synchronous.h"
#include "comm/inifile.h"
#include "JSON\json.h"
#include "redis.h"
#include <iostream>
#include <conio.h>
#include <stdio.h>
#include <time.h>
#include <stdlib.h>
#include <cpprest/http_client.h>
#include <strstream>

#include "fdcmdline.h"
#include <map>
#ifdef _WIN32
#include <Windows.h>
#include <process.h>
#define THREAD HANDLE

#else
#include <unistd.h>
#include <pthread.h>
#define STDCALL 
#define Sleep(n) usleep(1000*n);
#define THREAD pthread_t

#endif


#ifdef _WIN64  
#ifdef _DEBUG  
#pragma comment(lib, "..\\k2r\\lib\\x64\\Debug\\libjason.lib")
#pragma comment(lib, "..\\k2r\\lib\\x64\\Debug\\cpprestsdk.lib")
#else
#pragma comment(lib, "..\\k2r\\lib\\x64\\Release\\libjason.lib")
#pragma comment(lib, "..\\k2r\\lib\\x64\\Release\\cpprestsdk.lib")
#endif
#else  
#ifdef _DEBUG  
#pragma comment(lib, "..\\k2r\\lib\\x86\\Debug\\libjason.lib")
#pragma comment(lib, "..\\k2r\\lib\\x86\\Debug\\cpprestsdk.lib")
#else
#pragma comment(lib, "..\\k2r\\lib\\x86\\Release\\libjason.lib")
#pragma comment(lib, "..\\k2r\\lib\\x86\\Release\\cpprestsdk.lib")
#endif
#endif  

time_t str2sec(string str);
int commitdata(const char* s);
std::map<std::string, int> g_pt_status;

#ifdef _WIN32
int gettimeofday(struct timeval *tp, void *tzp)
{
	time_t clock;
	struct tm tm;
	SYSTEMTIME wtm;
	GetLocalTime(&wtm);
	tm.tm_year = wtm.wYear - 1900;
	tm.tm_mon = wtm.wMonth - 1;
	tm.tm_mday = wtm.wDay;
	tm.tm_hour = wtm.wHour;
	tm.tm_min = wtm.wMinute;
	tm.tm_sec = wtm.wSecond;
	tm.tm_isdst = -1;
	clock = mktime(&tm);
	if (clock < 0) throw "monitor():gettimeofday()";
	tp->tv_sec = clock;
	tp->tv_usec = wtm.wMilliseconds * 1000;
	return (0);
}
#endif

long long getCurrentTimeByMil()
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return tv.tv_sec * 1000LL + tv.tv_usec / 1000LL;
}

char g_RedisIp[128] = "127.0.0.1";
int g_RedisPort = 6379;
char g_BrokerList[128] = "127.0.0.1:9092";
char g_GroupId[256] = "RT_DATA";
int g_ThreadCnt = 10;
char g_KafkaTopic[256] = "RT_DATA";
char g_tag_prefix[200] = "ACQDATA_R_";
/////////////////////////////调试开关//////////////////////////////////////
bool bcrazy = false;
bool bdubug_v = false;
//////////////////////////////////////////////////////////////////////////
typedef struct th_info_t
{
	bool bstate;//true:run  false:terminate
	uint64_t ppackage;//处理kafka消息条数
	uint64_t precorde;//处理点位数据数量
	long long begintm;//开始处理时间
	unsigned int threadid;
	THREAD thhd;
};

map<unsigned int, th_info_t*> g_ThreadList;

void simulate(string& s)
{
	stringstream ss;
	time_t tt;
	tm t;
	time(&tt);
	localtime_s(&t, &tt);
	static int st = 0;
	st = st > 0 ? 0 : 1;
	ss << "{\"MSG_HEAD\":\"begin\",\"MSG_ORG\" : \"SH_JMDX\",\"MSG_PRJ\" : \"SH_JMDX_1\",\"MSG_SYS\" : \"BA\","
		<<"\"MSG_DEV\" : \"BA\",\"MSG_TYPE\" : \"dh_u\",\"MSG_ID\" : \"20151109 - 091011\",\"MSG_SEG\":\"0\",\"MSG_NUM\" : \" - 1\",";

	ss << "\"MSG_BODY\":["; 
	for (int i = 0; i < 10000;i++)
	{
		ss << "{\"ptname\":\"tag" << i << "\",\"pttime\":\"" << t.tm_year + 1900 << "-" << t.tm_mon + 1 << "-"
			<< t.tm_mday << " " << t.tm_hour << ":" << t.tm_min << ":" << t.tm_sec << "\",\"ptvalue\":\"111\",\"ptstatus\":\""<<st<<"\"},";
	}
	s = ss.str();
	s[s.size() - 1] = ']';
	s += "}";
	

}

#ifdef _WIN32
unsigned __stdcall save_to_redis(void* pobj)
#else
void* process_data(void* pobj)
#endif
{

}
#ifdef _WIN32
unsigned __stdcall process_data(void* pobj)
#else
void* process_data(void* pobj)
#endif
{
	th_info_t* pinfo = (th_info_t*)pobj;
	//连接kafka
	vector<string> topic;
	topic.push_back(g_KafkaTopic);
	
	map<string, string> conf;
	//map<string, string> conf_p;

	conf.insert(pair<string, string>("metadata.broker.list", g_BrokerList));
	conf.insert(pair<string, string>("group.id", g_GroupId));

	map<string, string> tconf;
	kafka_consumer_allot k2r_c;
	//kafka_producer k2r_p;

	k2r_c.loadconfig(conf, tconf);
	k2r_c.create_topic(topic);

	//k2r_p.loadconfig(conf, conf_p);
	//k2r_p.create_topic("HIS_DATA");

	//连接redis
	COperateData rediscon;
	rediscon.InitOperateData(g_RedisIp, g_RedisPort,5);
	if (!rediscon.Connect())
	{
		cout << "thread[" << pinfo->threadid << "] conect to redis failed" << endl;
	}
	

	Json::Value vb;
	Json::Value v;
	Json::Reader r;

	string strJson = "";
	string strkey = "";
	string strvalue = "";
	stringstream ss;
	
	pinfo->bstate = true;
	pinfo->ppackage = 0;
	pinfo->precorde = 0;

	pinfo->begintm = getCurrentTimeByMil();
	vector<pair<string,string>> dtls;
	while (pinfo->bstate)
	{
	
		strJson = k2r_c.consume();
		//simulate(strJson);
		//strJson = "{\"MSG_BODY\":[{\"ptname\":\"aaa\",\"pttime\":\"bbb\",\"ptvalue\":\"111\"},{\"ptname\":\"aaa2\",\"pttime\":\"bbb2\",\"ptvalue\":\"222\"}]}";
		if (strJson.empty())
		{
			Sleep(100);
			continue;
		}
		if (bcrazy)
		{
			pinfo->ppackage++;
			continue;
		}
		pinfo->ppackage++;
		long long bt = getCurrentTimeByMil();
		if (!r.parse(strJson,vb) || !vb.isMember("MSG_BODY") ||!vb["MSG_BODY"].isArray())
		{
			cout << "bad json:" << strJson << endl;
			Sleep(2);
			continue;
		}
		long long et = getCurrentTimeByMil();
		if (bdubug_v)
		{
			cout << "parse --> " << et - bt << "\n<";
		}
		bt = getCurrentTimeByMil();
		v = vb["MSG_BODY"];
		dtls.clear();
		string tagname = "";
		string pttime = "";
		string ptstatus = "";
		string ptvalue = "";
		string org = "";
		string site = "";
		string sys = "";
		if(vb.isMember("MSG_ORG")) org = vb["MSG_ORG"].asString();
		if (vb.isMember("MSG_PRJ")) site = vb["MSG_PRJ"].asString();
		if (vb.isMember("MSG_SYS")) sys = vb["MSG_SYS"].asString();
// 		Json::FastWriter w;
// 		Json::Value wv;
// 		Json::Value wbody;
// 		wv["MSG_HEAD"] = "begin";
// 		wv["MSG_ORG"] = vb["MSG_ORG"];
// 		wv["MSG_PRJ"] = vb["MSG_PRJ"];
// 		wv["MSG_SYS"] = vb["MSG_SYS"];
// 		wv["MSG_DEV"] = vb["MSG_DEV"];
// 		wv["MSG_TYPE"] = "dh_u";
// 		wv["MSG_ID"] = "end";
// 		wv["MSG_SEG"] = "0";
// 		wv["MSG_NUM"] = "-1";
// 		//wv["MSG_BODY"] = "end";
// 		wv["MSG_TAIL"] = "end";
		string httpbody = "";
		for (int i = 0; i < v.size(); i++)
		{
			tagname = v[i]["ptname"].asString();
			pttime = v[i]["pttime"].asString();
			ptstatus = v[i]["ptstatus"].asString();
			ptvalue = v[i]["ptvalue"].asString();
			

			strkey = g_tag_prefix + tagname;
			if (v[i].isMember("ptstatus") && "" != org && "" != site && "" != sys)
			{
				strvalue = tagname + ",0," + pttime + "," + ptvalue + ","+ ptstatus;
				ss.clear();
				ss.str("");
				ss << ptstatus;
				int st = 0;
				time_t pt_timestamp = str2sec(pttime)*1000;
				if(ss>>st && pt_timestamp > 0 )
				{	
					st = (st>0) ? 2 : 1;

					int& oldstatus = g_pt_status[tagname];
					if (oldstatus != st)
					{
						oldstatus = st;
						//////////////////////////////////////////////////////////////////////////
						ss.clear();
						ss.str("");
						ss << "{\"name\": \"pt_abn_record\",\"timestamp\" : " << pt_timestamp << ",\"value\" :" << ptvalue << ",\"tags\" : {\"tagname\": \""<<tagname<<"\",\"ORG\": \""
							<< org << "\",\"PRJ\": \"" << site << "\",\"SYS\": \"" << sys << "\"}}";
						httpbody += ss.str() + ",";

						ss.clear();
						ss.str("");
						ss << "{\"name\": \"pt_abn_record\",\"timestamp\" : " << pt_timestamp << ",\"value\" :" << ptstatus << ",\"tags\" : {\"snapshot\": \""<<tagname<<"\",\"ORG\": \""
							<< org << "\",\"PRJ\": \"" << site << "\",\"SYS\": \"" << sys << "\"}}";
						httpbody += ss.str() + ",";
					}
				}
				
				
			}
			else
				strvalue = v[i]["ptname"].asString() +",0," + v[i]["pttime"].asString()+","+ v[i]["ptvalue"].asString() + ",0";
			dtls.push_back(pair<string, string>(strkey, strvalue));
			//rediscon.Push(strkey, strvalue);
			pinfo->precorde++;
		}
		//wv["MSG_BODY"] = wbody;
		if (dtls.size() > 0)
		{
			rediscon.PushMultiData(dtls);
		}
		if (httpbody.size() > 0)
		{
			httpbody = "[" + httpbody;
			httpbody[httpbody.size() - 1] = ']';
			commitdata(httpbody.c_str());
		}
		et = getCurrentTimeByMil();
		if (bdubug_v)
		{
			cout << "redis --> " << et - bt << "\n<";
		}
		v.clear();
		vb.clear();
		Sleep(100);
	}
	long long endtm = getCurrentTimeByMil();
	cout << "thread[" << pinfo->threadid << "] will terminated: \n\tstart-time: " << "2016-06-22" << "\n\tprocess package:" << pinfo->ppackage
		<< "\n\tprocess record:" << pinfo->precorde << "\n\ttotoal time:" << (endtm- pinfo->begintm)/3600000<<":"<< ((endtm - pinfo->begintm) % 3600000)/60000.<<":" 
		<< ((endtm - pinfo->begintm) % 60000) / 1000.<<"."<< (endtm - pinfo->begintm) % 1000 << "\n\tavg time for package(r/s):" << pinfo->ppackage *1000/(endtm - pinfo->begintm)
		<<"\n\tavg time for record(p/s):" << pinfo->precorde * 1000. / (endtm - pinfo->begintm) <<endl;
#ifdef _WIN32
	return (unsigned)pobj;
#else
	return pobj;;
#endif
}

void add_thread(int n)
{
	if (n > 0)
	{
		for (int i = 0; i < n; i++)
		{
			th_info_t* pinfo = new th_info_t();
			pinfo->bstate = true;
			pinfo->thhd = (THREAD)_beginthreadex(NULL, 0, &process_data, (void*)(pinfo), 0, &pinfo->threadid);
			g_ThreadList[pinfo->threadid] = pinfo;
		}
		
	}
	else if(n < 0)
	{
		for (int i = 0; i < -n; i++)
		{
			map<unsigned int, th_info_t*>::iterator it = g_ThreadList.begin();
			if (it == g_ThreadList.end()) break;
			it->second->bstate = false;
			if (WAIT_OBJECT_0 != WaitForSingleObject(it->second->thhd, INFINITE))
			{
				cout << "terminate thread[" << it->second->threadid << "] failed" << endl;
			}
			delete it->second;
			g_ThreadList.erase(it);
			
		}
	}
	//g_ThreadCnt += n;
}

char kairosdb[128] = "127.0.0.1:8080";

int main(int argc, char **argv)
{

	if (initSocket() < 0)
	{
		printf("Init Socket Failed!\n");
		return -1;
	}

	char ch ;
	string str = app_path();
	str += "\\k2r.ini";

#if 0
	map<string, string> conf;
	conf.insert(pair<string, string>("metadata.broker.list", "127.0.0.1:9092"));
	conf.insert(pair<string, string>("group.id", "abd"));

	map<string, string> tconf;
	kafka_consumer_allot* k2r_c = new kafka_consumer_allot();
	kafka_consumer_allot* k2r_c2 = new kafka_consumer_allot();
	k2r_c->loadconfig(conf, tconf);
	vector<string> dd;
	dd.push_back("dfsfdf");
	k2r_c->create_topic(dd);

	k2r_c2->loadconfig(conf, tconf);
	k2r_c2->create_topic(dd);
	delete k2r_c;
	k2r_c2->consume();

	delete k2r_c2;


#else
	//initSocket 启动Redis
	

	read_profile_string(str.c_str(), "sys", "redis-ip", g_RedisIp, "127.0.0.1");
	read_profile_int(str.c_str(), "sys", "redis-port",  &g_RedisPort, 6379);
	read_profile_string(str.c_str(), "sys", "tag-prefix", g_tag_prefix, "ACQDATA_R_");
	read_profile_string(str.c_str(), "sys", "topic", g_KafkaTopic,"RD_DATA");
	read_profile_string(str.c_str(), "sys", "broker-list", g_BrokerList, "127.0.0.1:9092");;
	read_profile_string(str.c_str(), "sys", "group-id", g_GroupId, "KFTP_K2R"); 
	read_profile_int(str.c_str(), "sys", "thread-count", &g_ThreadCnt, 1);

	read_profile_string(str.c_str(), "sys", "kairosdb", kairosdb, "127.0.0.1:8080");
	

	add_thread(g_ThreadCnt);
	stringstream ss;
	ss << "Redis: " << g_RedisIp << ":" << g_RedisPort << "\nKakfa: \n\t" << "host: " << g_BrokerList << "\n\tdata topic: " << g_KafkaTopic << "\n\tgroup: " << g_GroupId
		<< "\nThread count:" << g_ThreadList.size()<<"\ntag prefix: "<<g_tag_prefix;
	Sleep(2000);
	init_cmdline_evn(ss.str());
	string strcmd = "";
	while (get_cmdline(strcmd))
	{
		if (0 == strcmd.find("addthd"))
		{
			strcmd = strcmd.substr(strlen("addthd"));
			ss.str("");
			ss.clear();
			ss << strcmd;
			int cnt = 0;
			ss >> cnt;
			add_thread(cnt);
			ss.str("");
			ss.clear();
			ss << g_ThreadList.size() + cnt;
			write_profile_string("sys", "thread-count", ss.str().c_str(), str.c_str());
			continue;
		}
		if ("q" == strcmd)
		{
			break;
		}
		if ("info" == strcmd)
		{
			ss.str("");
			ss.clear();
			ss << "thread count: " << g_ThreadList.size();
			long long curtm = getCurrentTimeByMil();
			for (map<unsigned int,th_info_t*>::iterator it = g_ThreadList.begin(); it != g_ThreadList.end(); it++)
			{
				ss << "\n[" << it->first << "]: \nstatus = " << it->second->bstate << ", packge = " << it->second->ppackage << ", record = "
					<< it->second->precorde << ", time(ms) = " << curtm - it->second->begintm << ", avg-package = " << it->second->ppackage * 1000. / (curtm - it->second->begintm)
					<< ", avg-record = " << it->second->precorde * 1000. / (curtm - it->second->begintm) << "\n";
			}
			cout << ss.str() << "<";
			continue;;
		}
		if ("crazy" == strcmd)
		{
			bcrazy = true;
			continue;
		}
		if ("crazy_dis" == strcmd)
		{
			bcrazy = false;
			continue;
		}
		if ("v" == strcmd)
		{
			bdubug_v = true;
			continue;
		}
		if ("v_dis" == strcmd)
		{
			bdubug_v = false;
			continue;
		}
	}

	
#endif

	return 0;
}

#else

#endif

time_t str2sec(string str)
{
	stringstream ss(str);
	tm t = { 0 };
	if(6 == sscanf_s(str.c_str(),"%d-%d-%d %d:%d:%d", &t.tm_year , &t.tm_mon, &t.tm_mday , &t.tm_hour, &t.tm_min , &t.tm_sec))
	//if (ss >> t.tm_year >> t.tm_mon >> t.tm_mday >> t.tm_hour >> t.tm_min >> t.tm_sec)
	{
		t.tm_year -= 1900;
		t.tm_mon -= 1;
		return mktime(&t);
	}
	return -1;
}


int commitdata(const char* s)
{
	//printf("%s\n",s);
	int ret = 0;
	char ur[255] = { 0 };
	sprintf_s(ur, "http://%s", kairosdb);
	//http_client client(web::uri(ur));  
	//U("http://192.168.1.148:8086")
	string str = ur;
	std::wstring wstr(strlen(ur), L' ');
	std::copy(str.begin(), str.end(), wstr.begin());
	//wstr
#ifdef _MSC_VER
	web::http::client::http_client client(wstr);
#else
	utility::string_t addr = ur;
	web::uri svrcri(addr);
	http_client client(svrcri);
#endif	



	utf8string uir = "/api/v1/datapoints";


	//Concurrency::task<http_response> response = client.request(methods::POST, uir,single, types);	
	web::http::http_response   response = client.request(web::http::methods::POST, uir, s).get();
	ret = response.status_code();
	if (ret != 204)
	{
		printf("Write  points ERROR:  %u returned.\n", ret);
		//	LogMessage("Write  points ERROR:  %u returned. wait 3 s ,try again \n", ret);
		//Sleep(3000);
		//	http_response   response = client.request(methods::POST, uir, s).get();
		//		ret = response.status_code();
		//	if (ret != 204)
		//		{
		//			printf("Write  points ERROR AGAIN:  %u returned.\n", ret);
		//			LogMessage("Write  points ERROR AGAIN:  %u returned. \n", ret);
		//		}
		char info[50];
		sprintf(info, "Write  points ERROR:  %u returned.", ret);
		//LogMessage(info);
	}
	/*else
	{
	printf("commit success\n");
	char info[50];
	sprintf(info, "commit success" );
	LogMessage(info);
	}*/
	return ret;

}