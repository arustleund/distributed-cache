package com.rustleund.dcchallenge.distributedcache.node.impl;

import com.rustleund.dcchallenge.distributedcache.node.Node;
import com.rustleund.dcchallenge.distributedcache.node.NodeConnection;
import com.rustleund.dcchallenge.distributedcache.node.NodeConnectionFactory;
import com.rustleund.dcchallenge.distributedcache.node.NodeType;

/**
 * A simple {@link NodeConnectionFactory}, always returns a {@link SimpleNodeConnection}
 */
public class SimpleNodeConnectionFactory implements NodeConnectionFactory {

	@Override
	public <KeyT, ValueT> NodeConnection<KeyT, ValueT> findNodeConnectionForNodeType(Node node) {
		// Just using the same kind of connection for each type here, but could use different connections for each node
		if (node.getType() == NodeType.A) {
			return new SimpleNodeConnection<>(node);
		}
		if (node.getType() == NodeType.B) {
			return new SimpleNodeConnection<>(node);
		}
		throw new IllegalArgumentException("Unknown NodeType, cannot find connection: " + node.getType());
	}
}