package com.rustleund.dcchallenge.hashinglookup;

import com.google.common.hash.Funnel;

/**
 * A data structure for performing lookups by a hash value. Values are stored in the lookup, and can be retrieved by passing in
 * any other value that can be hashed. Details on replication of values and hash methods are left to the implementation.
 *
 * @param <ValueT> The type of values stored in the lookup
 */
public interface HashingLookup<ValueT> {

	/**
	 * Store a value in the lookup. Value will be indexed by its hash (hash method left to implementation).
	 *
	 * @param value The value to store in the lookup
	 */
	void storeValue(ValueT value);

	/**
	 * Remove a value from the lookup, if present. Values will not be removed unless their {@link Object#equals(Object)} method returns true for the value passed to this method.
	 *
	 * @param value The value to remove from the lookup. Must return true for {@link Object#equals(Object)} with the values in the lookup to be properly removed.
	 */
	void removeValue(ValueT value);

	/**
	 * Find a value in the lookup by some key. A {@link Funnel} must be provided to calculate a hash for the key.
	 * The method by which to find a key will vary by implementation but most implementations should return a "best"
	 * match for any key as long as there are values in the lookup. {@code null} will be returned if there are no
	 * matches or the lookup is empty
	 *
	 * @param key The key to find a value for in the lookup
	 * @param keyFunnel A {@link Funnel} to create a hash for the key
	 * @param <KeyT> The type of the key
	 * @return The value that best matches the key in the lookup, or {@code null} if no match is found or the lookup is empty
	 */
	<KeyT> ValueT lookupValue(KeyT key, Funnel<? super KeyT> keyFunnel);
}
