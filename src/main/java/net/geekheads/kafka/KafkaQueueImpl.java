package net.geekheads.kafka;

import java.util.Properties;

import net.geekheads.queue.QueueException;
import net.geekheads.queue.TimedQueueImpl;
import kafka.consumer.KafkaStream;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

public class KafkaQueueImpl extends TimedQueueImpl {
	private static final String STRING_ENCODER = StringEncoder.class.getName();
	private static final String SERIALIZER_CLASS = "serializer.class";
	private String topic;
	private KafkaStringClient client = new KafkaStringClient();
	private KafkaStream<String, String> stream;
	private Producer<String, String> producer;

	public void setGroupid(String groupid) {
		client.setGroupid(groupid);
	}

	public void setZkConnectString(String connectString) {
		client.setZkConnectString(connectString);
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	private KafkaStream<String, String> getStream() {
		if (stream == null) {
			stream = client.getStreams(topic, 1).get(0);
		}
		return stream;
	}
	
	private Producer<String, String> getProducer() {
		if (producer == null) {
			Properties properties = client.getProperties();
			properties.setProperty(SERIALIZER_CLASS, STRING_ENCODER);
			producer = new Producer<String, String>(new ProducerConfig(properties));
		}
		
		return producer;
	}

	public long getDefaultQueueTimeout() {
		// TODO Auto-generated method stub
		return 0;
	}

	public String get(long timeout) throws QueueException {
		return getStream().iterator().next().message();
	}

	public void put(String value) {
		getProducer().send(new KeyedMessage<String, String>(topic, value));
	}

	public void shutdown() {
		if (client != null) client.shutdown();
	}

}
