package com.taobao.iblc.api;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.TaskGroup;
import com.taobao.iblc.conf.IBLConfig;
import com.taobao.iblc.pojo.DataPack;
import com.taobao.iblc.shard.RuleService;


public class BatchHandler {
	
	protected final static Logger logger = LoggerFactory.getLogger(BatchHandler.class);
	
	private String tableName;//logic table name
	
	private Map<String,DataPack> dataMap = null;
	private TaskGroup queueMap = null;
	private RuleService rule = null;
	private int count = 0;
	
	
	public RuleService getRule() {
		return rule;
	}

	public void setRule(RuleService rule) {
		this.rule = rule;
	}


	public TaskGroup getQueueMap() {
		return queueMap;
	}

	public void setQueueMap(TaskGroup queueMap) {
		this.queueMap = queueMap;
	}
	
	public int begin(String tblName){
		this.tableName = tblName;
		this.dataMap = new HashMap<String,DataPack>();
		this.count = 0;
		return 0;
	}

//	public int addRow(List<String> row){ 
//		String actualTableName = calcActualTableName(row);
//		DataPack dp = dataMap.get(actualTableName);
//		if(dp == null){
//			StringBuilder sb = getStringBuilder();
//			dp = new DataPack(tableName,actualTableName,sb);
//			//dp = new DataPack(tableName,actualTableName);
//			dataMap.put(actualTableName, dp);
//		}
//		dp.addRow(row);
//		this.count++;
//		return count;
//	}
	
	public int addRow(String[] row){ 
		String actualTableName = calcActualTableName(row);
		DataPack dp = dataMap.get(actualTableName);
		if(dp == null){
			StringBuilder sb = getStringBuilder();
			dp = new DataPack(tableName,actualTableName,sb);
			//dp = new DataPack(tableName,actualTableName);
			dataMap.put(actualTableName, dp);
		}
		dp.addRow(row);
		this.count++;
		return count;
	}
	
	private StringBuilder getStringBuilder() {
		 return new StringBuilder(IBLConfig.blockSize);
	}

//	public String calcActualTableName(List<String> row){		
//		return rule.calcActualTableName(tableName,row);
//	}
	
	public String calcActualTableName(String[] row){		
		return rule.calcActualTableName(tableName,row);
	}
	
	/**
	 * blocking commit
	 * @return
	 *
	 * @throws UnsupportedEncodingException
	 * @throws InterruptedException 
	 */
	public boolean commit() throws UnsupportedEncodingException, InterruptedException{
		boolean result = false;
		for(Map.Entry<String,DataPack> entry : dataMap.entrySet()){
			DataPack dp = entry.getValue();
			dp.setBuf(dp.getData().toString().getBytes(IBLConfig.charset));
			//logger.info("buf size: "+dp.getBuf().length);
			dp.setData(null);
			queueMap.putTask(dp);
		}
		result = true;
		return result;
	}
	/**
	 * non-blocking commit
	 * @param time unit ms
	 * @return
	 * @throws UnsupportedEncodingException
	 * @throws InterruptedException
	 */
	public boolean commit(long time) throws UnsupportedEncodingException, InterruptedException { 
		boolean result = false;
		for(Map.Entry<String,DataPack> entry : dataMap.entrySet()){
			DataPack dp = entry.getValue();
			dp.setBuf(dp.getData().toString().getBytes(IBLConfig.charset));
			//logger.info("buf size: "+dp.getBuf().length);
			dp.setData(null);
			result = queueMap.putTask(dp,time);
			if(result == false)
				return false;
		}
		return true;
	}
	
	public boolean commit(long timeout,ResultProcessor rp){
		return false;
	}
	
	
	public List<String> next(){
		return null;	
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	

}
