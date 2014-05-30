package net.geekheads.kafka;

import net.geekheads.kafka.KafkaQueueImpl;

import net.geekheads.queue.Queue;
import net.geekheads.queue.StringSerializer;

public class KafkaQueueImplTest {

	private static final String TEST_STRING = "testing";

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		KafkaQueueImpl impl = new KafkaQueueImpl();
		impl.setGroupid("test_group");
		impl.setZkConnectString("localhost:2181");
		Queue<String> queue = new Queue<String>(impl, new StringSerializer());
		try {
			for (int i = 0; i < 100; ++i) {
				queue.put(TEST_STRING);
			}
			for (int i = 0; i < 100; ++i) {
				String ret = queue.get(Queue.BLOCK_INDEFINITELY);
				if (!ret.equals(TEST_STRING)) {
					System.err.println("Got wrong string '" + ret + "'");
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			queue.shutdown();
		}
	}

}
