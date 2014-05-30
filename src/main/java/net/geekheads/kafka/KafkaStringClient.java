package net.geekheads.kafka;

import java.util.List;

import kafka.consumer.KafkaStream;
import kafka.serializer.StringDecoder;

public class KafkaStringClient extends KafkaClient<String, String> {
	public List<KafkaStream<String, String>> getStreams(String topic, int numThreads) {
		return getStreams(topic, numThreads, new StringDecoder(null), new StringDecoder(null));
	}
}
