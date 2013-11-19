package com.taobao.iblc;

import java.util.concurrent.ConcurrentLinkedQueue;

public class StringBuilderCache {
	private static ConcurrentLinkedQueue<StringBuilder> cache = new ConcurrentLinkedQueue<StringBuilder>();
	
	public static void init(int capacity,int blockSize) {
		for(int i =0;i<capacity;i++){
			StringBuilder sb= new StringBuilder(blockSize);
			cache.add(sb);
		}
	}
	
	public static StringBuilder getStringBuilder(){
		return cache.poll();
	}
	
	public static void reuseStringBuilder(StringBuilder sb){
		cache.add(sb);
	}

}
