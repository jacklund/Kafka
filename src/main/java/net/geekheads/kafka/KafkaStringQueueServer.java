package net.geekheads.kafka;

import kafka.serializer.StringDecoder;

public class KafkaStringQueueServer extends KafkaQueueServer<String, String> {
	public KafkaStringQueueServer() {
		setKeyDecoder(new StringDecoder(null));
		setValueDecoder(new StringDecoder(null));
	}
}
