package com.rustleund.dcchallenge.distributedcache;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hashing;
import com.rustleund.dcchallenge.distributedcache.node.Node;
import com.rustleund.dcchallenge.distributedcache.node.NodeType;
import com.rustleund.dcchallenge.distributedcache.node.impl.SimpleNodeConnectionFactory;
import com.rustleund.dcchallenge.hashinglookup.HashingLookup;
import com.rustleund.dcchallenge.hashinglookup.impl.ConsistentHashingLookup;

/**
 * A test client to demonstrate functionality of the {@link DistributedCache}
 */
public class DistributedCacheExampleTestClient {

	private static final Logger LOG = LoggerFactory.getLogger(DistributedCacheExampleTestClient.class);

	public static void main(String[] args) throws IOException {
		HashingLookup<Node> hashingLookup = new ConsistentHashingLookup<>(4, Hashing.murmur3_32(), node -> node.getNodeId().toString());
		DistributedCache<String, String> cache = new DistributedCache<>(hashingLookup, Funnels.stringFunnel(Charset.defaultCharset()), new SimpleNodeConnectionFactory());

		LOG.info("Get a value when no values and no nodes added");
		LOG.info("Should be null: {}", cache.get("A"));

		LOG.info("Invalidate a value when no values and no nodes added");
		cache.invalidate("A"); // Should do nothing

		LOG.info("Putting value when no nodes added");
		cache.put("A", "AValue"); // Does nothing

		Node node1 = new Node(UUID.randomUUID(), "host1", 1, NodeType.A);
		LOG.info("Adding node {}", node1.getNodeId());
		cache.nodeAdded(node1);

		LOG.info("Putting value with an node");
		cache.put("A", "AValue");

		LOG.info("Get value back from cache");
		LOG.info("Should be 'AValue': {}", cache.get("A"));

		LOG.info("Invalidate value");
		cache.invalidate("A");

		LOG.info("Get value back from cache");
		LOG.info("Should be null: {}", cache.get("A"));

		LOG.info("Add a few more nodes");
		Node node2 = new Node(UUID.randomUUID(), "host2", 2, NodeType.B);
		LOG.info("Adding node {}", node2.getNodeId());
		cache.nodeAdded(node2);
		Node node3 = new Node(UUID.randomUUID(), "host3", 3, NodeType.A);
		LOG.info("Adding node {}", node3.getNodeId());
		cache.nodeAdded(node3);

		LOG.info("Add a few values");
		cache.put("B", "BValue");
		cache.put("C", "CValue");
		cache.put("D", "DValue");
		cache.put("E", "EValue");

		LOG.info("Get the values back out");
		LOG.info("Should be 'BValue': {}", cache.get("B"));
		LOG.info("Should be 'CValue': {}", cache.get("C"));
		LOG.info("Should be 'DValue': {}", cache.get("D"));
		LOG.info("Should be 'EValue': {}", cache.get("E"));

		LOG.info("Shutting down node {}", node2.getNodeId());
		cache.nodeShuttingDown(node2);
		LOG.info("Removing node {}", node2.getNodeId());
		cache.nodeRemoved(node2);

		LOG.info("Values should have been redistributed and still present");
		LOG.info("Should be 'BValue': {}", cache.get("B"));
		LOG.info("Should be 'CValue': {}", cache.get("C"));
		LOG.info("Should be 'DValue': {}", cache.get("D"));
		LOG.info("Should be 'EValue': {}", cache.get("E"));

		LOG.info("Shutting down node {}", node3.getNodeId());
		cache.nodeShuttingDown(node3);
		LOG.info("Removing node {}", node3.getNodeId());
		cache.nodeRemoved(node3);

		LOG.info("Values should have been redistributed and still present");
		LOG.info("Should be 'BValue': {}", cache.get("B"));
		LOG.info("Should be 'CValue': {}", cache.get("C"));
		LOG.info("Should be 'DValue': {}", cache.get("D"));
		LOG.info("Should be 'EValue': {}", cache.get("E"));

		LOG.info("Shutting down node {}", node1.getNodeId());
		cache.nodeShuttingDown(node1);
		LOG.info("Removing node {}", node1.getNodeId());
		cache.nodeRemoved(node1);

		LOG.info("Values are lost, there were no nodes to redistribute to");
		LOG.info("Should be null: {}", cache.get("B"));
		LOG.info("Should be null: {}", cache.get("C"));
		LOG.info("Should be null: {}", cache.get("D"));
		LOG.info("Should be null: {}", cache.get("E"));

		LOG.info("Add a node and remove it without shutting down");
		LOG.info("Adding node {}", node2.getNodeId());
		cache.nodeAdded(node2);
		LOG.info("Add a value");
		cache.put("B", "BValue");
		LOG.info("Removing node {}", node2.getNodeId());
		cache.nodeRemoved(node2);
	}
}
