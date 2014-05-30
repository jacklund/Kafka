package net.geekheads.zookeeper;

import org.kohsuke.args4j.Option;

public class ZooKeeperConfig {
	private static final String DEFAULT_CONNECT_STRING = "localhost:2181";

	private String connectString = DEFAULT_CONNECT_STRING;
	private int connectionTimeoutMs = -1;
	private int sessionTimeoutMs = -1;
	private int synctimeMs = -1;

	public String getConnectString() {
		return connectString;
	}

	@Option(name = "--zkConnect", usage = "specifies the zookeeper connection string in the form hostname:port/chroot")
	public void setConnectString(String connectString) {
		this.connectString = connectString;
	}

	public int getConnectionTimeoutMs() {
		return connectionTimeoutMs;
	}

	@Option(name = "--zkConnectionTimeoutMs", usage = "the max time that the client waits to establish a connection to zookeeper")
	public void setConnectionTimeoutMs(int connectionTimeoutMs) {
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public int getSessionTimeoutMs() {
		return sessionTimeoutMs;
	}

	@Option(name = "--zkSessionTimeoutMs", usage = "zookeeper session timeout")
	public void setSessionTimeoutMs(int sessionTimeoutMs) {
		this.sessionTimeoutMs = sessionTimeoutMs;
	}

	public int getSynctimeMs() {
		return synctimeMs;
	}

	@Option(name = "--zkSyncTimeMs", usage = "how far a ZK follower can be behind a ZK leader")
	public void setSynctimeMs(int synctimeMs) {
		this.synctimeMs = synctimeMs;
	}

}
