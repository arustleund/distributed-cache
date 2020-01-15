package com.rustleund.dcchallenge.distributedcache.node.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rustleund.dcchallenge.distributedcache.node.Node;
import com.rustleund.dcchallenge.distributedcache.node.NodeConnection;

/**
 * A simple implementation of {@link NodeConnection} that simply stores values locally in a Map
 *
 * @param <KeyT> The type of the keys of the map
 */
public class SimpleNodeConnection<KeyT, ValueT> implements NodeConnection<KeyT, ValueT> {

	private static final Logger LOG = LoggerFactory.getLogger(SimpleNodeConnection.class);

	private final Map<KeyT, ValueT> store = new HashMap<>();

	private final Node node;

	public SimpleNodeConnection(Node node) {
		this.node = node;
	}

	@Override
	public void put(KeyT key, ValueT value) {
		LOG.info("Putting key {} in node {}", key, node.getNodeId());
		store.put(key, value);
	}

	@Override
	public ValueT get(KeyT key) {
		LOG.info("Retrieving value for key {} from node {}", key, node.getNodeId());
		return store.get(key);
	}

	@Override
	public void invalidate(KeyT key) {
		LOG.info("Invalidating value for key {} in node {}", key, node.getNodeId());
		store.remove(key);
	}

	@Override
	public Stream<KeyT> keys() {
		return new HashSet<>(store.keySet()).stream();
	}

	@Override
	public void open() {
		LOG.info("Opening connection for node: {}", node.getNodeId());
	}

	@Override
	public void close() {
		LOG.info("Closing connection for node: {}", node.getNodeId());
		store.clear();
	}
}
