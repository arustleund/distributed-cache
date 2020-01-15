package com.rustleund.dcchallenge.distributedcache.node;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Represents a connection interface to a RemoteCache node
 *
 * @param <KeyT> The type of keys to store
 * @param <ValueT> The type of values to store
 */
public interface NodeConnection<KeyT, ValueT> extends Closeable {

	/**
	 * Open a connection to the node that this connection is for
	 *
	 * @throws IOException If a communication error occurs
	 */
	void open() throws IOException;

	/**
	 * Store a key/value pair into the node
	 *
	 * @param key The key to store
	 * @param value The value to store
	 * @throws IOException If a communication error occurs
	 */
	void put(KeyT key, ValueT value) throws IOException;

	/**
	 * Get the value currently stored in this node for the given key, or {@code null} if there is no value
	 *
	 * @param key The key to lookup a value for
	 * @return The value under the given key, or {@code null} if there is no value
	 * @throws IOException If a communication error occurs
	 */
	ValueT get(KeyT key) throws IOException;

	/**
	 * Invalidate the entry under the given key, if one exists
	 *
	 * @param key The key to invalidate the value for
	 * @throws IOException If a communication error occurs
	 */
	void invalidate(KeyT key) throws IOException;

	/**
	 * @return A {@link Stream} that includes all currently stored keys in this node
	 */
	Stream<KeyT> keys() throws IOException;
}
