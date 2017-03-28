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
			iseverity = event.severity();//��־���ؼ���
			strError = event.str();//��־��Ϣ�ַ�����
			strFac = event.fac(); //��־���ߴ���
			//fprintf(stderr, "LOG-%i-%s: %s\n", event.severity(), event.fac().c_str(), event.str().c_str());
			fprintf(stderr, "LOG-error\n");
			break;

		default:
			std::cerr << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
			break;
		}
	}
};


//DeliveryReportCb�ص���������һ�� ��RdKafka::Producer::produce() RdKafka::Message::err()
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
*���������ļ� ����kafka������(ȫ������)��topic�����á�KEY-VALUE ������metadata.broker.list--localhost:port ���������� topic������Ϊ��������
*@author	�ƾ�
*@inparam   kafkaconf	kafka������
*@inparam   topicconf	topic������
*@return    true:�ɹ� false:ʧ��
*/
bool kafka_producer::loadconfig(map<string, string> kafkaconf, map<string, string> topicconf)
{
	bool bRet = true;
	string errstr = "";	//������Ϣ Set configuration property �Ĵ�����Ϣ
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

	//�ص��������� ���õ����������е����á�
	
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
	//rd_kafka_conf_new() rd_kafka_topic_conf_new(); ����ʹ�ù����ǲ��ܱ��ٴ�ʹ�õģ������ں�������֮�󣬲���Ҫ�ͷ�������Դ��
	m_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	m_tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
}



kafka_producer::~kafka_producer()
{
	run = true;
	while (run && (m_producer->outq_len() > 0)) //outq_len()���س����е���Ϣ�� ��ǰ���г��ȴ���0����ѭ��
	{
		//�ö��а����ȴ����͵ģ�����ͨ��ȷ�ϵĴ������Ϣ�� Ӧ�ó���Ӧ�õȴ��˶�����ֹ��ȷ������(����ƫ�����ύ)��ȫ����
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
*����topic
*@author	�ƾ�
*@inparam   strtopic	topic�����ַ���
*@inparam   partition	������topicͬʱ�����������п���partitions��leader brokersӳ���ϵ
*@return    true:�ɹ� false:ʧ��
*/
bool kafka_producer::create_topic(string strtopic)
{
	string errstr = "";	//������Ϣ
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
			printf("���� �� ����������Ϣ������Ҫ�ٲ��룬����false\n");
			m_producer->poll(0);
			return false;
		}
	}

	if (m_producer->outq_len() > 10000)
	{
		printf("�ڴ�ռ�ù��࣬����produce\n");
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

	m_producer->poll(0);	//��ѯʱ��

	return true;
}
