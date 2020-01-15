package com.rustleund.dcchallenge;

import java.io.IOException;

/**
 * A simple remote cache interface. Methods throw {@link IOException} to allow clients to reasonably recover from network issues.
 *
 * @param <KeyT> The type of keys in the cache
 * @param <ValueT> The type of values in the cache
 */
public interface RemoteCache<KeyT, ValueT> {

	void put(KeyT key, ValueT value) throws IOException;

	ValueT get(KeyT key) throws IOException;

	void invalidate(KeyT key) throws IOException;
}
