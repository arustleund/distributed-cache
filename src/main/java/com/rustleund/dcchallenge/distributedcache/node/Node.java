package com.rustleund.dcchallenge.distributedcache.node;

import java.util.UUID;

public class Node {

	private UUID nodeId;
	private String hostname;
	private int port;
	private NodeType type;

	public Node(UUID nodeId, String hostname, int port, NodeType type) {
		this.nodeId = nodeId;
		this.hostname = hostname;
		this.port = port;
		this.type = type;
	}

	public UUID getNodeId() {
		return nodeId;
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}

	public NodeType getType() {
		return type;
	}
}
