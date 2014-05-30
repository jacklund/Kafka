package net.geekheads.kafka;

import net.geekheads.configuration.CommandLineParser;
import net.geekheads.configuration.JsonConfigFileParser;
import net.geekheads.kafka.KafkaQueueServer;
import net.geekheads.server.MessageProcessor;
import net.geekheads.server.MessageProcessorFactory;

import org.kohsuke.args4j.Option;

import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

public class DataCollectionServer {
	private static final String DEFAULT_GROUP_ID = "data_collection_group";
	private static final String DEFAULT_TOPIC = "data_collection";
	private static final Class<StringDecoder> DEFAULT_DECODER_CLASS = StringDecoder.class;
	
	private static String configFileName = null;
	private static CommandLineParser cmdLineParser;
	private static boolean isHelp = false;
	
	private static class Processor implements MessageProcessor<String> {
		public static class Factory implements MessageProcessorFactory<String> {
			public MessageProcessor<String> create() {
				return new Processor();
			}
		}
		public void process(String message) {
			System.out.println(message);
		}
	}

	@Option(name = "-C", aliases = {"--configFile"}, usage = "path to JSON-formatted configuration file")
	public void setConfigFileName(String name) {
		configFileName = name;
	}

	@Option(name = "-h", aliases = {"--help"}, usage = "display this message")
	public void usage(boolean dummy) {
		cmdLineParser.usage("DataCollectionServer", System.out);
		isHelp = true;
	}

	public static void main(String[] args) {
		KafkaQueueServer<String, String> server = new KafkaQueueServer<String, String>();
		cmdLineParser = null;
		try {
			// Set up the server defaults
			Decoder<String> decoder = DEFAULT_DECODER_CLASS.newInstance();
			server.setKeyDecoder(decoder);
			server.setValueDecoder(decoder);
			server.setTopic(DEFAULT_TOPIC);
			server.setGroupid(DEFAULT_GROUP_ID);

			// See if we have a config file name set on the command line
			cmdLineParser = new CommandLineParser(new DataCollectionServer(), server);
			cmdLineParser.parse(args);

			if (!isHelp) {
				// Let the config file override the defaults
				if (configFileName != null) {
					JsonConfigFileParser configParser = new JsonConfigFileParser(configFileName);
					configParser.configure(server);
				}
	
				// Let the command line parser override the config file entries
				cmdLineParser.parse(args);
	
				// Start the server
				server.start();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			cmdLineParser.usage("DataCollectionServer", System.err);
		}
	}
}
