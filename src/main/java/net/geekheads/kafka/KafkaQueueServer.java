package net.geekheads.kafka;

import java.util.List;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.kohsuke.args4j.Option;

import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import net.geekheads.server.MessageProcessor;
import net.geekheads.server.QueueServer;

public class KafkaQueueServer<K, V> extends QueueServer<V> {
	private static final int DEFAULT_NUM_THREADS = 4;
	private KafkaClient<K, V> client = new KafkaClient<K, V>();
	private String topic;
	private int numThreads;
	private Decoder<K> keyDecoder;
	private Decoder<V> valueDecoder;
	private Logger logger = Logger.getLogger(getClass());

	public class Worker<S, T> implements Runnable {
		KafkaStream<S, T> stream;
		
		public Worker(KafkaStream<S, T> s) {
			stream = s;
		}

		@SuppressWarnings("unchecked")
		public void run() {
			for (MessageAndMetadata<S, T> msgAndMetadata : stream) {
				try {
					((MessageProcessor<T>) holder.get()).process(msgAndMetadata.message());
				} catch (Exception e) {
					logger.error("Error processing message", e);
					logger.error("Message contents: " + msgAndMetadata.message());
				}
			}
		}
	}

	public KafkaQueueServer() {
		setNumThreads(DEFAULT_NUM_THREADS);
	}
	
	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public int getNumThreads() {
		return numThreads;
	}

	@Option(name = "--numThreads", usage = "Number of worker threads to spawn (default " + DEFAULT_NUM_THREADS + ")")
	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
		setPool(Executors.newFixedThreadPool(numThreads));
	}

	public KafkaClient<K, V> getClient() {
		return client;
	}

	public void setGroupid(String groupid) {
		client.setGroupid(groupid);
	}

	public void setZkConnectString(String connectString) {
		client.setZkConnectString(connectString);
	}

	public Decoder<K> getKeyDecoder() {
		return keyDecoder;
	}
	
	public void setKeyDecoder(Decoder<K> decoder) {
		this.keyDecoder = decoder;
	}

	public Decoder<V> getValueDecoder() {
		return valueDecoder;
	}
	
	public void setValueDecoder(Decoder<V> decoder) {
		this.valueDecoder = decoder;
	}
	
	@Override
	protected void run() {
		List<KafkaStream<K, V>> streams = null;
		try {
			streams = client.getStreams(topic, numThreads, keyDecoder, valueDecoder);
			logger.info("Have " + streams.size() + " streams listening to topic '" + topic + "'");
			for (KafkaStream<K, V> stream : streams) {
				pool.execute(new Worker<K, V>(stream));
			}
		} catch (Exception e) {
			logger.error("Error creating Kafka streams", e);
			client.shutdown();
		}
	}
}
