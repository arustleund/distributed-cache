package com.rustleund.dcchallenge.hashinglookup.impl;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashFunction;
import com.rustleund.dcchallenge.hashinglookup.HashingLookup;

/**
 * Implementation of {@link HashingLookup} that uses a consistent hashing algorithm, with replicas. Values will be turned into a string
 * for hashing purposes.
 *
 * @param <ValueT> The type of the value to store in the lookup
 */
public class ConsistentHashingLookup<ValueT> implements HashingLookup<ValueT> {

	private static final Funnel<? super String> VALUE_ID_FUNNEL = Funnels.stringFunnel(Charset.defaultCharset());

	private final List<ValueReplica> valueReplicas = new ArrayList<>();
	private final List<Integer> valueReplicaHashes = new ArrayList<>();

	private final int numberOfReplicas;
	private final HashFunction hashFunction;
	private final Function<? super ValueT, ? extends String> valueIdFunction;

	/**
	 * @param numberOfReplicas The number of replicas to put in the lookup for each value added, must be at least 1
	 * @param hashFunction     The {@link HashFunction} to use to hash values and keys for lookup, must produce hashes no more than 32 bits
	 * @param valueIdFunction  A {@link Function} to turn stored values into a String, will be used to help produce a hash for the value
	 */
	public ConsistentHashingLookup(int numberOfReplicas, HashFunction hashFunction, Function<? super ValueT, ? extends String> valueIdFunction) {
		checkArgument(numberOfReplicas >= 1, "Number of replicas must be greater than or equal to 1");
		checkArgument(hashFunction.bits() <= 32, "Hash Function must produce hashes less than or equal to 32 bits");
		this.numberOfReplicas = numberOfReplicas;
		this.hashFunction = hashFunction;
		this.valueIdFunction = valueIdFunction;
	}

	@Override
	public synchronized void storeValue(ValueT value) {
		IntStream.range(0, numberOfReplicas).forEach(replicaIndex -> storeValueReplica(value, replicaIndex));
	}

	private void storeValueReplica(ValueT value, int replicaIndex) {
		ValueReplica replica = new ValueReplica(value, replicaIndex);
		String replicaId = replica.toString();
		int replicaIdHashCode = hashFunction.hashObject(replicaId, VALUE_ID_FUNNEL).asInt();
		int insertionIndex = getInsertionIndex(replicaIdHashCode);
		valueReplicas.add(insertionIndex, replica);
		valueReplicaHashes.add(insertionIndex, replicaIdHashCode);
	}

	private int getInsertionIndex(int hashCode) {
		int binarySearchResult = Collections.binarySearch(valueReplicaHashes, hashCode);
		if (binarySearchResult < 0) {
			return (binarySearchResult + 1) * -1;
		}
		return binarySearchResult;
	}

	@Override
	public synchronized void removeValue(ValueT value) {
		Iterator<ValueReplica> replicaIterator = valueReplicas.iterator();
		Iterator<Integer> valueHashesIterator = valueReplicaHashes.iterator();
		int foundReplicas = 0;
		while (replicaIterator.hasNext() && foundReplicas < numberOfReplicas) {
			ValueReplica replica = replicaIterator.next();
			valueHashesIterator.next();
			if (replica.getValue().equals(value)) {
				foundReplicas++;
				replicaIterator.remove();
				valueHashesIterator.remove();
			}
		}
	}

	@Override
	public synchronized <KeyT> ValueT lookupValue(KeyT key, Funnel<? super KeyT> keyFunnel) {
		if (valueReplicas.isEmpty()) {
			return null;
		}
		int hashCode = hashFunction.hashObject(key, keyFunnel).asInt();
		int insertionIndex = getInsertionIndex(hashCode);
		if (insertionIndex == valueReplicas.size()) {
			insertionIndex = 0;
		}
		return valueReplicas.get(insertionIndex).getValue();
	}

	private class ValueReplica {

		private final ValueT value;
		private final int replicaIndex;

		ValueReplica(ValueT value, int replicaIndex) {
			this.value = value;
			this.replicaIndex = replicaIndex;
		}

		ValueT getValue() {
			return value;
		}

		@Override
		public String toString() {
			return valueIdFunction.apply(value) + "_" + replicaIndex;
		}
	}
}
