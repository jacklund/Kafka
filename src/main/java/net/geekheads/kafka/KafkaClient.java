package net.geekheads.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import net.geekheads.zookeeper.ZooKeeperConfig;

import org.kohsuke.args4j.Option;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.Decoder;

public class KafkaClient<K, V> {
	private Properties properties = new Properties();
	private ConsumerConnector consumerConnector = null;
	private ZooKeeperConfig zkConfig = new ZooKeeperConfig();

	public enum AutoOffsetReset {
		smallest,
		largest
	}

	private ConsumerConnector getConsumerConnector() {
		if (consumerConnector == null) {
			consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(getProperties()));
		}
		
		return consumerConnector;
	}

	public Properties getProperties() {
		setPropertiesFromZKConfig();
		return properties;
	}

	private void setPropertiesFromZKConfig() {
		if (zkConfig.getConnectString() != null) setProperty("zk.connect", zkConfig.getConnectString());
		if (zkConfig.getConnectionTimeoutMs() > -1) setProperty("zk.connectiontimeout.ms", Integer.toString(zkConfig.getConnectionTimeoutMs()));
		if (zkConfig.getSessionTimeoutMs() > -1) setProperty("zk.sessiontimeout.ms", Integer.toString(zkConfig.getSessionTimeoutMs()));
		if (zkConfig.getSynctimeMs() > -1) setProperty("zk.synctime.ms", Integer.toString(zkConfig.getSynctimeMs()));
	}

	public void setProperty(String key, String value) {
		properties.setProperty(key, value);
	}
	
	public ZooKeeperConfig getZkConfig() {
		return zkConfig;
	}

	public void setZkConfig(ZooKeeperConfig config) {
		zkConfig = config;
	}
	
	@Option(name = "--kafkaAutocommit", usage = "if true, periodically commit to zookeeper " +
			"the offset of messages already fetched by the consumer")
	public void setAutocommit(boolean value) {
		setProperty("autocommit.enable", Boolean.toString(value));
	}
	
	@Option(name = "--kafkaAutocommitIntervalMs", usage = "the frequency in ms that the consumer " +
			"offsets are committed to zookeeper")
	public void setAutocommitIntervalMs(int value) {
		setProperty("autocommit.interval.ms", Integer.toString(value));
	}
	
	@Option(name = "--kafkaAutoOffsetReset", usage = "if \"smallest\", automatically reset the " +
			"offset to the smallest offset available on the broker, if \"largest\", automatically " +
			"reset the offset to the largest offset available on the broker")
	public void setAutoOffsetReset(AutoOffsetReset value) {
		setProperty("autooffset.reset", value.name());
	}
	
	@Option(name = "--kafkaConsumerTimeoutMs", usage = "message timeout in ms, -1 means block indefinitely")
	public void setConsumerTimeoutMs(int value) {
		setProperty("consumer.timeout.ms", Integer.toString(value));
	}

	@Option(name = "--kafkaFetchSize", usage = "the number of byes of messages to attempt to fetch")
	public void setFetchSize(int value) {
		setProperty("fetch.size", Integer.toString(value));
	}
	
	@Option(name = "--kafkaBackoffIncrementMs", usage = "to avoid repeatedly polling a broker node which has no new data we will " +
			"backoff every time we get an empty set from the broker")
	public void setBackoffIncrementMs(long value) {
		setProperty("backoff.increment.ms", Long.toString(value));
	}
	
	@Option(name = "--kafkaGroupid", usage = "a string that uniquely identifies a set of consumers within the same consumer group")
	public void setGroupid(String groupid) {
		setProperty("groupid", groupid);
	}
	
	@Option(name = "--kafkaMaxQueuedChunks", usage = "max number of messages buffered for consumption")
	public void setMaxQueuedChunks(int value) {
		setProperty("queuedchunks.max", Integer.toString(value));
	}
	
	@Option(name = "--kafkaMaxRebalanceRetries", usage = "max number of retries during rebalance")
	public void setMaxRebalanceRetries(int value) {
		setProperty("rebalance.retries.max", Integer.toString(value));
	}
	
	@Option(name = "--kafkaSocketBufferSize", usage = "the socket receive buffer size for network requests")
	public void setSocketBufferSize(int value) {
		setProperty("buffer.size", Integer.toString(value));
	}
	
	@Option(name = "--kafkaSocketTimeoutMs", usage = "The socket timeout in milliseconds")
	public void setSocketTimeoutMs(int value) {
		setProperty("socket.timeout.ms", Integer.toString(value));
	}

	public void setZkConnectString(String connectString) {
		zkConfig.setConnectString(connectString);
	}

	public void setZkConnectionTimeoutMs(int value) {
		zkConfig.setConnectionTimeoutMs(value);
	}
	
	public void setZkSessionTimeoutMs(int value) {
		zkConfig.setSessionTimeoutMs(value);
	}
	
	public void setZkSyncTimeMs(int value) {
		zkConfig.setSynctimeMs(value);
	}
	
	public List<KafkaStream<K, V>> getStreams(String topic, int numThreads, Decoder<K> keyDecoder, Decoder<V> valueDecoder) {
		Map<String, Integer> streamMap = new HashMap<String, Integer>();
		streamMap.put(topic, numThreads);
		Map<String, List<KafkaStream<K, V>>> topicMessageStreams;
		topicMessageStreams = getConsumerConnector().createMessageStreams(streamMap, keyDecoder, valueDecoder);
		return topicMessageStreams.get(topic);
	}

	public void shutdown() {
		if (consumerConnector != null) consumerConnector.shutdown();
	}
}
