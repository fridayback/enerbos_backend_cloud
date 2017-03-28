#include "kafka_producer.h"
#include <Windows.h>
#include <time.h> 
#include <iostream>
#include <csignal>
using namespace std;


bool run = true;
static void sigterm(int sig) 
{
	run = false;
}



class ExampleEventCbProducer : public RdKafka::EventCb 
{
public:
	void event_cb(RdKafka::Event &event) 
	{
		string strError;
		int iseverity;
		string strFac;
		switch (event.type())
		{
		case RdKafka::Event::EVENT_ERROR:
			 strError = event.str();
			cout<<strError<<endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				run = false;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			iseverity = event.severity();//日志严重级别。
			strError = event.str();//日志消息字符串。
			strFac = event.fac(); //日志工具串。
			//fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(), event.str().c_str());
			fprintf(stderr, "LOG-error\n");
			break;

		default:
			std::cerr << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
			break;
		}
	}
};


//DeliveryReportCb回调将被调用一次 当RdKafka::Producer::produce() RdKafka::Message::err()
class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb 
{
public:
	void dr_cb(RdKafka::Message &message) 
	{
		std::cout << "Message delivery for (" << message.len() << " bytes): " << message.errstr() << std::endl;
		if (!run)
		{
			run = true;
		}
	}
};


ExampleEventCbProducer ex_event_cb;
ExampleDeliveryReportCb ex_dr_cb;//Set delivery report callback

/**
*加载配置文件 加载kafka的配置(全局配置)和topic的配置。KEY-VALUE 举例：metadata.broker.list--localhost:port 这样的配置 topic的配置为单独配置
*@author	黄静
*@inparam   kafkaconf	kafka的配置
*@inparam   topicconf	topic的配置
*@return    true:成功 false:失败
*/
bool kafka_producer::loadconfig(map<string, string> kafkaconf, map<string, string> topicconf)
{
	bool bRet = true;
	string errstr = "";	//错误信息 Set configuration property 的错误信息
	map<string, string>::iterator it;

	//Set configuration properties
	for (it = kafkaconf.begin(); it != kafkaconf.end(); ++it)
	{
		//cout << "key: " << it->first << " value: " << it->second << endl;
		if (m_conf->set(it->first, it->second, errstr) != RdKafka::Conf::CONF_OK)
		{
			std::cerr << errstr << std::endl;
			bRet = false;
		}
	}
	//Topic properties
	for (it = topicconf.begin(); it != topicconf.end(); ++it)
	{
		//cout << "key: " << it->first << " value: " << it->second << endl;
		if (m_tconf->set(it->first, it->second, errstr) != RdKafka::Conf::CONF_OK)
		{
			std::cerr << errstr << std::endl;
			bRet = false;
		}
	}

	//回调函数设置 调用的是在例子中的设置。
	
	m_conf->set("event_cb", &ex_event_cb, errstr);
	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);
	m_conf->set("dr_cb", &ex_dr_cb, errstr);

	//Create producer using accumulated global configuration.
	m_producer = RdKafka::Producer::create(m_conf, errstr);
	if (!m_producer)
	{
		//printf("Failed to create producerERROR %d-------%s(%d)", GetLastError(), __FILE__, __LINE__);
		std::cerr << "Failed to create producer: " << errstr << std::endl;
		return false;
	}

	std::cout << " Created producer " << m_producer->name() << std::endl;

	return bRet;
}


kafka_producer::kafka_producer()
{
	//rd_kafka_conf_new() rd_kafka_topic_conf_new(); 函数使用过后是不能被再次使用的，而且在函数调用之后，不需要释放配置资源。
	m_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	m_tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
}



kafka_producer::~kafka_producer()
{
	run = true;
	while (run && (m_producer->outq_len() > 0)) //outq_len()返回出队列的消息数 当前队列长度大于0进入循环
	{
		//该队列包含等待发送的，或者通过确认的代理的消息。 应用程序应该等待此队列终止，确保请求(例如偏移量提交)完全处理。
		std::cerr << "Waiting for " << m_producer->outq_len() << std::endl;
		m_producer->poll(1000);
	}

	delete m_conf;
	m_conf = NULL;
	delete m_tconf;
	m_tconf = NULL;
	delete m_topic;
	m_topic = NULL;
	delete m_producer;
	m_producer = NULL;
}

bool kafka_producer::init(string config, string param)
{
	return false;
}


/**
*创建topic
*@author	黄静
*@inparam   strtopic	topic名称字符串
*@inparam   partition	分区，topic同时还包含了所有可用partitions与leader brokers映射关系
*@return    true:成功 false:失败
*/
bool kafka_producer::create_topic(string strtopic)
{
	string errstr = "";	//错误信息
	//Create topic handle.
	m_topic = RdKafka::Topic::create(m_producer, strtopic, m_tconf, errstr);
	if (!m_topic)
	{
		std::cerr << "Failed to create topic: " << errstr << std::endl;
		return false;
	}
	//m_partition = partition;
	return true;
}




bool kafka_producer::product(const std::string data, int32_t iInpartition)
{
	if (data.empty())
	{
		m_producer->poll(0);
		return false;
	}
	if (!run)
	{
		if (m_producer->outq_len() > 0)
		{
			printf("离线 且 队列中有消息，不需要再插入，返回false\n");
			m_producer->poll(0);
			return false;
		}
	}

	if (m_producer->outq_len() > 10000)
	{
		printf("内存占用过多，不再produce\n");
		return false;
	}
	//int32_t ipartition = RdKafka::Topic::PARTITION_UA;
	//Produce message
	RdKafka::ErrorCode resp = m_producer->produce(m_topic, iInpartition, RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
		const_cast<char *>(data.c_str()), data.size(), NULL, NULL);
	if (resp != RdKafka::ERR_NO_ERROR)
	{
		std::cerr << " Produce failed: " << RdKafka::err2str(resp) << std::endl;
	}

	m_producer->poll(0);	//轮询时间

	return true;
}
