package com.taobao.iblc;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class QueueMapping {// only read no need lock
	private Map<String, BlockingQueue<DataPack>> mapping = new HashMap<String, BlockingQueue<DataPack>>();// actualTblname-queue

	public BlockingQueue<DataPack> getQueueByActualTableName(
			String actualTableName) {
		for (Map.Entry<String, BlockingQueue<DataPack>> entry : mapping
				.entrySet()) {
			if (actualTableName.startsWith(entry.getKey()))
				return entry.getValue();
		}
		return null;

	}

	public void put(String id, BlockingQueue<DataPack> queue) {
		mapping.put(id, queue);
	}

}
