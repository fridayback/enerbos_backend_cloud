

#include "rdkafkacpp.h"
#include <map>
#include <vector>
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


class kafka_consumer
{
	RdKafka::Consumer *m_consumer;
	RdKafka::Topic *m_topic;
	int32_t m_partition;
	RdKafka::Conf *m_conf;	//全局配置
	RdKafka::Conf *m_tconf;	//topic的配置
	
public:
	kafka_consumer();
	virtual ~kafka_consumer();
	bool loadconfig(std::map<std::string, std::string> kafkaconf, std::map<std::string, std::string> topicconf);
	bool init(std::string config, std::string param);
	bool create_topic(std::string strtopic, int partition);
	//bool destroy_topic(std::string strtopic, int partition);
	std::string consume();
};


class kafka_consumer_allot
{
	RdKafka::KafkaConsumer *m_consumer;
	//RdKafka::Topic *m_topic;
	//int32_t m_partition;
	RdKafka::Conf *m_conf;	//全局配置
	RdKafka::Conf *m_tconf;	//topic的配置

public:
	kafka_consumer_allot();
	virtual ~kafka_consumer_allot();
	bool loadconfig(std::map<std::string, std::string> kafkaconf, std::map<std::string, std::string> topicconf);
	bool init(std::string config, std::string param);
	bool create_topic(std::vector<std::string> strtopic);
	std::string consume();
};
