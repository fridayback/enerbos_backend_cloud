
//#include "rdkafka.h"
#include "rdkafkacpp.h"
#include <map>
#include <string>

#ifdef _WIN64  

#ifdef _DEBUG  
#pragma comment(lib, "..\\x64\\Debug\\librdkafka.lib")
#pragma comment(lib, "..\\x64\\Debug\\librdkafkacpp.lib")
#else
#pragma comment(lib, "..\\x64\\Release\\librdkafka.lib")
#pragma comment(lib, "..\\x64\\Release\\librdkafkacpp.lib")
#endif

#else  

#ifdef _DEBUG  
#pragma comment(lib, "..\\..\\build_win\\win32_debug\\libkafka.lib")
#pragma comment(lib, "..\\..\\build_win\\win32_debug\\libkafkacpp.lib")
#else
//#pragma comment(lib, "..\\..\\build_win\\win32_release\\libkafka.lib")
//#pragma comment(lib, "..\\..\\build_win\\win32_release\\libkafkacpp.lib")

#pragma comment(lib, "..\\Release\\librdkafka.lib")
#pragma comment(lib, "..\\Release\\librdkafkacpp.lib")
#endif


#endif  


//Producer����Ϣ��������ָ����topic��,����������������ĸ�����,ͨ���ض��ķ�������ѡ�����
class kafka_producer
{
	RdKafka::Producer *m_producer;
	RdKafka::Topic *m_topic;
	RdKafka::Conf *m_conf;	//ȫ������
	RdKafka::Conf *m_tconf;	//topic������
public:
	kafka_producer();
	virtual ~kafka_producer();
	bool loadconfig(std::map<std::string, std::string> kafkaconf, std::map<std::string, std::string> topicconf);
	bool init(std::string config, std::string param);
	bool create_topic(std::string strtopic);
	//bool destroy_topic(std::string strtopic, int partiion);
	bool product(const std::string data, int32_t iInpartition = -1);
};
