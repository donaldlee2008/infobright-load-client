package com.taobao.iblc;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.conf.IBLConfig;
import com.taobao.iblc.pojo.DataPack;

public class TaskGroup {
	protected final static Logger logger = LoggerFactory
			.getLogger(TaskGroup.class);
	
	private ConcurrentHashMap<String, BlockingQueue<DataPack>> table_mapping = new ConcurrentHashMap<String, BlockingQueue<DataPack>>();// actualTblname-queue
    
	private List<Thread> loaderTL = new ArrayList<Thread>();
	private List<IBLoaderNew> ibloaderListNew = new ArrayList<IBLoaderNew>();
	
	
	//blocking
	public int putTask(DataPack dp) throws InterruptedException{
		BlockingQueue<DataPack> bqueue = getQueueByActualTableName(dp.getActualTableName());

		bqueue.put(dp); 

		return 0;	
	}
	
	public boolean putTask(DataPack dp,long time) throws InterruptedException{ 
		boolean result = false;
		BlockingQueue<DataPack> bqueue = getQueueByActualTableName(dp.getActualTableName());
	    result = bqueue.offer(dp, time, TimeUnit.MILLISECONDS);
	    if(bqueue.size()>=IBLConfig.loadQueueSize/4)
		    logger.info(dp.getActualTableName()+" Queue size: " + bqueue.size());
		return result;	
		
	}
	
	public BlockingQueue<DataPack> getQueueByActualTableName(
			String actualTableName) {
		// String key = actualTableName.substring(0,actualTableName.lastIndexOf('_'));
		BlockingQueue<DataPack> queue = table_mapping.get(actualTableName);
		if (queue == null) {
			synchronized (this) {
				queue = table_mapping.get(actualTableName);
				if(queue != null)
					return queue;
				queue = new ArrayBlockingQueue<DataPack>(
						IBLConfig.loadQueueSize);
				table_mapping.put(actualTableName, queue);
				IBLoaderNew ibl = new IBLoaderNew(actualTableName, queue);
				Thread t = new Thread(ibl, "IBLoader-" + actualTableName);
				ibloaderListNew.add(ibl);
				loaderTL.add(t);
				t.start();
			}
		}
		return queue;
	}
	
//	public void printQueueSize(){
//		for(Map.Entry<String,BlockingQueue<DataPack>> entry : table_mapping.entrySet()){
//			
//		}
//	}
	
	public void close() { 
		synchronized (this) {
			for (Thread t : loaderTL) {
				t.interrupt();
			}
		}
	}

	
	public static void main(String args[]) {
		String actualTableName = "eagleeye.eaglelog_0_10";
		String str = actualTableName.substring(0,actualTableName.lastIndexOf('_'));
		System.out.println(str);
	}
	

}
