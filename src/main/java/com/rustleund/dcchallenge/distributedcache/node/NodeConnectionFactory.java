package com.rustleund.dcchallenge.distributedcache.node;

/**
 * See {@link #findNodeConnectionForNodeType(Node)}
 */
public interface NodeConnectionFactory {

	/**
	 * Make a new {@link NodeConnection} for a given {@link Node}. Connections returned will not be opened.
	 *
	 * @param node The {@link Node} to make a {@link NodeConnection} for
	 * @param <KeyT> The type of keys stored in the node connection
	 * @param <ValueT> The type of values stored in the node connection
	 * @return A new {@link NodeConnection} for the given {@link Node}, not opened
	 */
	<KeyT, ValueT> NodeConnection<KeyT, ValueT> findNodeConnectionForNodeType(Node node);
}
