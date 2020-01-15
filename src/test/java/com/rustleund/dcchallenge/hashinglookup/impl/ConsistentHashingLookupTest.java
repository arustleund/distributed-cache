package com.rustleund.dcchallenge.hashinglookup.impl;

import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.util.function.Function;

import org.junit.Test;

import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class ConsistentHashingLookupTest {

	@Test
	public void integratedTest() {
		HashFunction hashFunction = Hashing.murmur3_32();
		Funnel<CharSequence> stringFunnel = Funnels.stringFunnel(Charset.defaultCharset());
		Function<? super String, ? extends String> valueIdFunction = Function.identity();
		ConsistentHashingLookup<String> testee = new ConsistentHashingLookup<>(2, hashFunction, valueIdFunction);

		// Test depends upon known hash values, listed in hash order
		assertEquals(-1516818811, hash(hashFunction, stringFunnel, "C_1"));
		assertEquals(30097145, hash(hashFunction, stringFunnel, "E_1"));
		assertEquals(655182097, hash(hashFunction, stringFunnel, "B_1"));
		assertEquals(756905339, hash(hashFunction, stringFunnel, "E_0"));
		assertEquals(1063065488, hash(hashFunction, stringFunnel, "D_1"));
		assertEquals(1371772626, hash(hashFunction, stringFunnel, "A_0"));
		assertEquals(1381442483, hash(hashFunction, stringFunnel, "D_0"));
		assertEquals(1787141165, hash(hashFunction, stringFunnel, "A_1"));
		assertEquals(1787741025, hash(hashFunction, stringFunnel, "C_0"));
		assertEquals(2123861949, hash(hashFunction, stringFunnel, "B_0"));

		// test empty returns null
		assertNull(testee.lookupValue("A_0", stringFunnel));

		testee.storeValue("A");
		// Any key lookup should return "A", as it's the only value
		assertEquals("A", testee.lookupValue("A_0", stringFunnel));
		assertEquals("A", testee.lookupValue("A_1", stringFunnel));
		assertEquals("A", testee.lookupValue("B_0", stringFunnel));
		assertEquals("A", testee.lookupValue("B_1", stringFunnel));
		assertEquals("A", testee.lookupValue("C_0", stringFunnel));
		assertEquals("A", testee.lookupValue("C_1", stringFunnel));

		testee.storeValue("B");
		// Now find the next value in clockwise order
		assertEquals("A", testee.lookupValue("A_0", stringFunnel));
		assertEquals("A", testee.lookupValue("A_1", stringFunnel));
		assertEquals("B", testee.lookupValue("B_0", stringFunnel));
		assertEquals("B", testee.lookupValue("B_1", stringFunnel));
		assertEquals("B", testee.lookupValue("C_0", stringFunnel));
		assertEquals("B", testee.lookupValue("C_1", stringFunnel));
		assertEquals("A", testee.lookupValue("D_0", stringFunnel));
		assertEquals("A", testee.lookupValue("D_1", stringFunnel));
		assertEquals("A", testee.lookupValue("E_0", stringFunnel));
		assertEquals("B", testee.lookupValue("E_1", stringFunnel));

		// Three values
		testee.storeValue("C");
		assertEquals("A", testee.lookupValue("A_0", stringFunnel));
		assertEquals("A", testee.lookupValue("A_1", stringFunnel));
		assertEquals("B", testee.lookupValue("B_0", stringFunnel));
		assertEquals("B", testee.lookupValue("B_1", stringFunnel));
		assertEquals("C", testee.lookupValue("C_0", stringFunnel));
		assertEquals("C", testee.lookupValue("C_1", stringFunnel));
		assertEquals("A", testee.lookupValue("D_0", stringFunnel));
		assertEquals("A", testee.lookupValue("D_1", stringFunnel));
		assertEquals("A", testee.lookupValue("E_0", stringFunnel));
		assertEquals("B", testee.lookupValue("E_1", stringFunnel));

		// Back to two values, removing B
		testee.removeValue("B");
		assertEquals("A", testee.lookupValue("A_0", stringFunnel));
		assertEquals("A", testee.lookupValue("A_1", stringFunnel));
		assertEquals("C", testee.lookupValue("B_0", stringFunnel));
		assertEquals("A", testee.lookupValue("B_1", stringFunnel));
		assertEquals("C", testee.lookupValue("C_0", stringFunnel));
		assertEquals("C", testee.lookupValue("C_1", stringFunnel));
		assertEquals("A", testee.lookupValue("D_0", stringFunnel));
		assertEquals("A", testee.lookupValue("D_1", stringFunnel));
		assertEquals("A", testee.lookupValue("E_0", stringFunnel));
		assertEquals("A", testee.lookupValue("E_1", stringFunnel));
	}

	private int hash(HashFunction hashFunction, Funnel<CharSequence> stringFunnel, String s) {
		return hashFunction.hashObject(s, stringFunnel).asInt();
	}
}