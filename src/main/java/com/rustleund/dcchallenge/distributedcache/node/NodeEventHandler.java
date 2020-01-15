package com.rustleund.dcchallenge.distributedcache.node;

public interface NodeEventHandler {

	void nodeAdded(Node newNode);
	void nodeRemoved(Node node);
	void nodeShuttingDown(Node node);
}
