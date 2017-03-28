

#include "kafka_consumer.h"
#include <iostream>
#include <time.h>
#include <csignal>
using namespace std;

static int partition_cnt = 0;
static int eof_cnt = 0;

static bool exit_eof = false;
//static bool run = true;
extern bool run;
static void sigterm(int sig)
{
	run = false;
}


class ExampleRebalanceCb : public RdKafka::RebalanceCb 
{
private:
	static void part_list_print(const std::vector<RdKafka::TopicPartition*>&partitions) 
	{
		for (unsigned int i = 0; i < partitions.size(); i++)
		{
			std::cerr << partitions[i]->topic() << "[" << partitions[i]->partition() << "], ";
		}
		std::cerr << "\n";
	}

public:
	void rebalance_cb(RdKafka::KafkaConsumer *consumer, RdKafka::ErrorCode err, std::vector<RdKafka::TopicPartition*> &partitions) 
	{
		std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

		part_list_print(partitions);

		if (err == RdKafka::ERR__ASSIGN_PARTITIONS) 
		{
			consumer->assign(partitions);
			partition_cnt = partitions.size();
		}
		else 
		{
			consumer->unassign();
			partition_cnt = 0;
		}
		eof_cnt = 0;
	}
};


void msg_consume(RdKafka::Message* message, void* opaque) 
{
	switch (message->err()) 
	{
	case RdKafka::ERR__TIMED_OUT:
		break;

	case RdKafka::ERR_NO_ERROR:
		/* Real message */
		//std::cout << "Read msg at offset " << message->offset() << std::endl;
		message->offset();
		if (message->key()) 
		{
			std::cout << "Key: " << *message->key() << std::endl;
		}
		printf("%.*s\n", static_cast<int>(message->len()), static_cast<const char *>(message->payload()));
		break;

	case RdKafka::ERR__PARTITION_EOF:
		/* Last message */
		if (exit_eof) 
		{
			std::cerr << "%% EOF reached for all " << partition_cnt <<
				" partition(s)" << std::endl;
			run = false;
		}
		break;

	case RdKafka::ERR__UNKNOWN_TOPIC:
	case RdKafka::ERR__UNKNOWN_PARTITION:
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
		break;

	default:
		/* Errors */
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
	}
}

class ExampleConsumeCb : public RdKafka::ConsumeCb 
{
public:
	void consume_cb(RdKafka::Message &msg, void *opaque) 
	{
		msg_consume(&msg, opaque);
	}
};

//事件是从librdkafka应用程序传播的错误，统计数据，日志等通用接口。
class ExampleEventCb : public RdKafka::EventCb
{
public:
	void event_cb(RdKafka::Event &event)
	{
		switch (event.type())
		{
		case RdKafka::Event::EVENT_ERROR:
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				run = false;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s: %s\n",event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		default:
			std::cerr << "EVENT " << event.type() <<" (" << RdKafka::err2str(event.err()) << "): " <<event.str() << std::endl;
			break;
		}
	}
};
ExampleRebalanceCb ex_rebalance_cb_Consumer;
ExampleEventCb ex_event_cb_Consumer;

bool kafka_consumer::loadconfig(map<string, string> kafkaconf, map<string, string> topicconf)
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

	
	m_conf->set("rebalance_cb", &ex_rebalance_cb_Consumer, errstr);
	m_conf->set("event_cb", &ex_event_cb_Consumer, errstr);
	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	//Consumer mode
	//Create consumer using accumulated global configuration.
	m_consumer = RdKafka::Consumer::create(m_conf, errstr);
	if (!m_consumer) 
	{
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		return false;
	}

	//std::cout << " Created consumer " << m_consumer->name() << std::endl;
	return bRet;
}


kafka_consumer::kafka_consumer()
{
	m_partition = RdKafka::Topic::PARTITION_UA;
	//rd_kafka_conf_new() rd_kafka_topic_conf_new(); 函数使用过后是不能被再次使用的，而且在函数调用之后，不需要释放配置资源。
	m_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	m_tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
}
kafka_consumer::~kafka_consumer()
{
	//Stop consumer
	m_consumer->stop(m_topic, m_partition);
	m_consumer->poll(1000);

	delete m_conf;
	m_conf = NULL;
	delete m_tconf;
	m_tconf = NULL;
	delete m_topic;
	m_topic = NULL;
	delete m_consumer;
	m_consumer = NULL;
}
bool kafka_consumer::init(std::string config, std::string param)
{
	//可以配置读取的偏移量 是否使用回调等等参数
	return false;
}
bool kafka_consumer::create_topic(std::string strtopic, int partition)
{
	m_partition = partition;
	string errstr = "";
	//Create topic handle.
	m_topic = RdKafka::Topic::create(m_consumer, strtopic, m_tconf, errstr);
	if (!m_topic)
	{
		std::cerr << "Failed to create topic: " << errstr << std::endl;
		return false;
	}
	//消费者读取的偏移量
	int64_t start_offset = RdKafka::Topic::OFFSET_END; //RdKafka::Topic::OFFSET_END  RdKafka::Topic::OFFSET_BEGINNING  RdKafka::Topic::OFFSET_STORED
	//Start consumer for topic+partition at start offset
	RdKafka::ErrorCode resp = m_consumer->start(m_topic, m_partition, start_offset);
	if (resp != RdKafka::ERR_NO_ERROR) 
	{
		std::cerr << "Failed to start consumer: " << RdKafka::err2str(resp) << std::endl;
		return false;
	}

	return true;
}

string kafka_consumer::consume()
{
	string strRet = "";
	char *strTmp;
	ExampleConsumeCb ex_consume_cb;
	//是否使用回调函数
	int use_ccb = 0;
	//Consume messages
	//while (run) 
	{
		if (use_ccb) 
		{
			m_consumer->consume_callback(m_topic, m_partition, 1000, &ex_consume_cb, &use_ccb);
		}
		else 
		{
			RdKafka::Message *msg = m_consumer->consume(m_topic, m_partition, 1000);
			//msg_consume(msg, NULL);

			switch (msg->err()) 
			{
			case RdKafka::ERR__TIMED_OUT:
				break;

			case RdKafka::ERR_NO_ERROR:
				/* Real message */
				//std::cout << "Read msg at offset " << message->offset() << std::endl;
				msg->offset();
				if (msg->key()) 
				{
					std::cout << "Key: " << *msg->key() << std::endl;
				}
				//printf("%.*s\n", static_cast<int>(message->len()), static_cast<const char *>(message->payload()));
				
				strTmp = new char[static_cast<int>(msg->len()) + 1];
				memset(strTmp, 0, static_cast<int>(msg->len()) + 1);
				memcpy(strTmp, static_cast<const char *>(msg->payload()), static_cast<int>(msg->len()));
				strRet = strTmp;
				//printf("%s\n", strRet.c_str());
				delete strTmp;
				strTmp = NULL;
				break;

			case RdKafka::ERR__PARTITION_EOF:
				/* Last message */
				if (exit_eof) 
				{
					run = false;
				}
				break;

			case RdKafka::ERR__UNKNOWN_TOPIC:
			case RdKafka::ERR__UNKNOWN_PARTITION:
				std::cerr << "Consume failed: " << msg->errstr() << std::endl;
				run = false;
				break;

			default:
				/* Errors */
				std::cerr << "Consume failed: " << msg->errstr() << std::endl;
				run = false;
			}
			delete msg;
		}
		m_consumer->poll(0);
	}

	return strRet;
}





































































bool kafka_consumer_allot::loadconfig(map<string, string> kafkaconf, map<string, string> topicconf)
{
	bool bRet = true;
	string errstr = "";	//错误信息 Set configuration property 的错误信息
	map<string, string>::iterator it;

	//m_conf->set("default_topic_conf", m_tconf, errstr);

	//Set configuration properties
	for (it = kafkaconf.begin(); it != kafkaconf.end(); ++it)
	{
		if (m_conf->set(it->first, it->second, errstr) != RdKafka::Conf::CONF_OK)
		{
			std::cerr << errstr << std::endl;
			bRet = false;
		}
	}
	//Topic properties
	for (it = topicconf.begin(); it != topicconf.end(); ++it)
	{
		if (m_tconf->set(it->first, it->second, errstr) != RdKafka::Conf::CONF_OK)
		{
			std::cerr << errstr << std::endl;
			bRet = false;
		}
	}

	m_conf->set("rebalance_cb", &ex_rebalance_cb_Consumer, errstr);
	m_conf->set("event_cb", &ex_event_cb_Consumer, errstr);
	
	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	m_consumer = RdKafka::KafkaConsumer::create(m_conf, errstr);
	if (!m_consumer)
	{
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		return false;
	}

	return bRet;
}


kafka_consumer_allot::kafka_consumer_allot()
{
	//rd_kafka_conf_new() rd_kafka_topic_conf_new(); 函数使用过后是不能被再次使用的，而且在函数调用之后，不需要释放配置资源。
	m_conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	m_tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
}
kafka_consumer_allot::~kafka_consumer_allot()
{
	//m_consumer->poll(1000);
	delete m_conf;
	m_conf = NULL;
	delete m_tconf;
	m_tconf = NULL;

//	m_consumer->close();
	delete m_consumer;
	m_consumer = NULL;
	RdKafka::wait_destroyed(5000);
}
bool kafka_consumer_allot::init(std::string config, std::string param)
{
	//可以配置读取的偏移量 是否使用回调等等参数
	return false;
}
bool kafka_consumer_allot::create_topic(vector<string> strtopic)
{
	//m_partition = partition;
	string errstr = "";
	//Create topic handle.
	/*for (int i = 0; i < strtopic.size(); i++)
	{
		m_topic = RdKafka::Topic::create(m_consumer, strtopic[i], m_tconf, errstr);
		if (!m_topic)
		{
			std::cerr << "Failed to create topic: " << errstr << std::endl;
			return false;
		}
	}*/
	
	//cout << strtopic[0] << endl;
	RdKafka::ErrorCode err = m_consumer->subscribe(strtopic);
	if (err)
	{
		std::cerr << "Failed to subscribe to " << strtopic.size() << " topics: "<< RdKafka::err2str(err) << std::endl;
		return false;
	}

	return true;
}

int verbosity = 1;

string kafka_consumer_allot::consume()
{
	string strRet = "";
	char *strTmp;
	ExampleConsumeCb ex_consume_cb;
	//是否使用回调函数
	int use_ccb = 0;
	//Consume messages
	//while (run) 
	{
		if (use_ccb)
		{
			//m_consumer->consume_callback(m_topic, m_partition, 1000, &ex_consume_cb, &use_ccb);
			std::cerr << "Use callback: Not implemented" << std::endl;
		}
		else
		{
			RdKafka::Message *msg = m_consumer->consume(0);
			//msg_consume(msg, NULL);

			switch (msg->err())
			{
			case RdKafka::ERR__TIMED_OUT:
				break;

			case RdKafka::ERR_NO_ERROR:
				/* Real message */

				
				//std::cerr << "Read msg at offset " << msg->offset() << std::endl;
				msg->offset();
				if (verbosity >= 2 && msg->key()) 
				{
					std::cout << "Key: " << *msg->key() << std::endl;
				}

				if (verbosity >= 1) 
				{
					//printf("%.*s\n",static_cast<int>(msg->len()),static_cast<const char *>(msg->payload()));

					strTmp = new char[static_cast<int>(msg->len()) + 1];
					memset(strTmp, 0, static_cast<int>(msg->len()) + 1);
					memcpy(strTmp, static_cast<const char *>(msg->payload()), static_cast<int>(msg->len()));
					strRet = strTmp;
					delete strTmp;
					strTmp = NULL;
				}



				//std::cout << "Read msg at offset " << message->offset() << std::endl;
			/*	msg->offset();
				if (msg->key())
				{
					std::cout << "Key: " << *msg->key() << std::endl;
				}
				//printf("%.*s\n", static_cast<int>(message->len()), static_cast<const char *>(message->payload()));

				strTmp = new char[static_cast<int>(msg->len()) + 1];
				memset(strTmp, 0, static_cast<int>(msg->len()) + 1);
				memcpy(strTmp, static_cast<const char *>(msg->payload()), static_cast<int>(msg->len()));
				strRet = strTmp;
				delete strTmp;
				strTmp = NULL;
				*/
				break;

			case RdKafka::ERR__PARTITION_EOF:
				/* Last message */
				if (exit_eof)
				{
					run = false;
				}
				break;

			case RdKafka::ERR__UNKNOWN_TOPIC:
			case RdKafka::ERR__UNKNOWN_PARTITION:
				std::cerr << "Consume failed: " << msg->errstr() << std::endl;
				run = false;
				break;

			default:
				/* Errors */
				std::cerr << "Consume failed: " << msg->errstr() << std::endl;
				run = false;
			}
			delete msg;
		}
		//m_consumer->poll(0);
	}

	return strRet;
}

