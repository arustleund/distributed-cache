package com.rustleund.dcchallenge.distributedcache;

import static com.rustleund.dcchallenge.util.LambdaUtil.acceptPropagate;
import static com.rustleund.dcchallenge.util.LambdaUtil.applyPropagate;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Funnel;
import com.rustleund.dcchallenge.RemoteCache;
import com.rustleund.dcchallenge.hashinglookup.HashingLookup;
import com.rustleund.dcchallenge.distributedcache.node.Node;
import com.rustleund.dcchallenge.distributedcache.node.NodeConnection;
import com.rustleund.dcchallenge.distributedcache.node.NodeConnectionFactory;
import com.rustleund.dcchallenge.distributedcache.node.NodeEventHandler;

/**
 * An implementation of {@link RemoteCache} that stores values on nodes in a network. Values are distributed to available nodes using a {@link HashingLookup} implementation.
 * Also implements {@link NodeEventHandler} to handle the addition and subtraction of nodes in the network.
 *
 * @param <KeyT> The type of the keys used to store values in the cache.
 */
public class DistributedCache<KeyT, ValueT> implements NodeEventHandler, RemoteCache<KeyT, ValueT> {

	private static final Logger LOG = LoggerFactory.getLogger(DistributedCache.class);

	private final Map<Node, NodeConnection<KeyT, ValueT>> nodeConnections = Collections.synchronizedMap(new HashMap<>());
	private final Funnel<? super KeyT> keyFunnel;
	private final HashingLookup<Node> hashingNodeLookup;
	private final NodeConnectionFactory nodeConnectionFactory;

	/**
	 * @param hashingLookup The {@link HashingLookup} to use to store {@link Node}s when they are added
	 * @param keyFunnel The {@link Funnel} to use to generate hashes for keys
	 * @param nodeConnectionFactory The {@link NodeConnectionFactory} to use to build connections for {@link Node}s when they are added
	 */
	public DistributedCache(HashingLookup<Node> hashingLookup, Funnel<? super KeyT> keyFunnel, NodeConnectionFactory nodeConnectionFactory) {
		this.hashingNodeLookup = hashingLookup;
		this.keyFunnel = keyFunnel;
		this.nodeConnectionFactory = nodeConnectionFactory;
	}

	@Override
	public void put(KeyT key, ValueT value) throws IOException {
		getNodeConnection(key).ifPresent(acceptPropagate(con -> con.put(key, value)));
	}

	private Optional<NodeConnection<KeyT, ValueT>> getNodeConnection(KeyT key) {
		return Optional.ofNullable(hashingNodeLookup.lookupValue(key, keyFunnel)).map(nodeConnections::get);
	}

	@Override
	public ValueT get(KeyT key) throws IOException {
		return getNodeConnection(key).map(applyPropagate(con -> con.get(key))).orElse(null);
	}

	@Override
	public void invalidate(KeyT key) throws IOException {
		getNodeConnection(key).ifPresent(acceptPropagate(con -> con.invalidate(key)));
	}

	@Override
	public void nodeAdded(Node newNode) {
		hashingNodeLookup.storeValue(newNode);
		NodeConnection<KeyT, ValueT> newNodeConnection = nodeConnectionFactory.findNodeConnectionForNodeType(newNode);
		try {
			newNodeConnection.open();
			nodeConnections.put(newNode, newNodeConnection);
			redistributeValues();
		} catch (IOException e) {
			LOG.error("Could not open a new connection for Node {}", newNode.getNodeId(), e);
		}
	}

	private void redistributeValues() {
		synchronized (nodeConnections) {
			nodeConnections.forEach(this::tryToRedistributeValuesFromNode);
		}
	}

	private void tryToRedistributeValuesFromNode(Node node, NodeConnection<KeyT, ValueT> nodeConnection) {
		try {
			Stream<KeyT> allKeysForNode = nodeConnection.keys();
			redistributeValuesFromNode(allKeysForNode, node, nodeConnection);
		} catch (IOException e) {
			LOG.error("Could not move values from Node {}", node.getNodeId(), e);
		}
	}

	private void redistributeValuesFromNode(Stream<KeyT> allKeysForNode, Node node, NodeConnection<KeyT, ValueT> nodeConnection) {
		allKeysForNode.forEach(key -> maybeRedistributeValue(key, node, nodeConnection));
	}

	private void maybeRedistributeValue(KeyT key, Node oldNode, NodeConnection<KeyT, ValueT> oldNodeConnection) {
		Node nodeWhereValueNowBelongs = hashingNodeLookup.lookupValue(key, keyFunnel);
		if (nodeWhereValueNowBelongs == null) {
			LOG.warn("Could not transfer value for key: {} from old node: {}, there are no active nodes to transfer to", key, oldNode.getNodeId());
		} else if (nodeWhereValueNowBelongs != oldNode) {
			// only move value if it now needs to be in a new node
			tryToRedistributeValue(key, nodeWhereValueNowBelongs, oldNode, oldNodeConnection);
		}
	}

	private void tryToRedistributeValue(KeyT key, Node newNode, Node oldNode, NodeConnection<KeyT, ValueT> oldNodeConnection) {
		tryToGetValueFromOldNode(key, oldNode, oldNodeConnection).ifPresent(value -> {
			NodeConnection<KeyT, ValueT> newNodeConnection = nodeConnections.get(newNode);
			tryToPutValueInNewNode(key, value, newNode, newNodeConnection);
			tryToInvalidateValueInOldNode(key, oldNode, oldNodeConnection);
		});
	}

	private Optional<ValueT> tryToGetValueFromOldNode(KeyT key, Node oldNode, NodeConnection<KeyT, ValueT> oldNodeConnection) {
		try {
			return Optional.ofNullable(oldNodeConnection.get(key));
		} catch (IOException e) {
			LOG.error("Could not redistribute value for key {}, could not get value from old node {}", key, oldNode.getNodeId(), e);
			return Optional.empty();
		}
	}

	private void tryToPutValueInNewNode(KeyT key, ValueT value, Node newNode, NodeConnection<KeyT, ValueT> newNodeConnection) {
		try {
			newNodeConnection.put(key, value);
		} catch (IOException e) {
			LOG.error("Could not put value for key {} in new node {}", key, newNode.getNodeId(), e);
		}
	}

	private void tryToInvalidateValueInOldNode(KeyT key, Node oldNode, NodeConnection<KeyT, ValueT> oldNodeConnection) {
		try {
			oldNodeConnection.invalidate(key);
		} catch (IOException e) {
			LOG.error("Could not invalidate value at key {} in old node {}", key, oldNode.getNodeId(), e);
		}
	}

	@Override
	public void nodeRemoved(Node node) {
		Optional.ofNullable(removeNode(node)).ifPresent(con -> {
			try {
				con.close();
			} catch (Exception e) {
				LOG.warn("Could not close connection to node {}", node.getNodeId(), e);
			}
		});
	}

	private NodeConnection<KeyT, ValueT> removeNode(Node node) {
		hashingNodeLookup.removeValue(node);
		return nodeConnections.remove(node);
	}

	@Override
	public void nodeShuttingDown(Node nodeToShutdown) {
		NodeConnection<KeyT, ValueT> nodeConnectionForNodeToShutdown = removeNode(nodeToShutdown);
		tryToRedistributeValuesFromNode(nodeToShutdown, nodeConnectionForNodeToShutdown);
		try {
			nodeConnectionForNodeToShutdown.close();
		} catch (IOException e) {
			LOG.warn("Could not close connection to node {}", nodeToShutdown.getNodeId(), e);
		}
	}
}


