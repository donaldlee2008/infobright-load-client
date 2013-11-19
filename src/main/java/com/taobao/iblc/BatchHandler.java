package com.taobao.iblc;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BatchHandler {
	
	protected final static Logger logger = LoggerFactory.getLogger(BatchHandler.class);
	
	private String tableName;//logic table name
	
	private Map<String,DataPack> dataMap = null;
	private QueueMapping queueMap = null;
	private RuleService rule = null;
	private int count = 0;
	
	
	public RuleService getRule() {
		return rule;
	}

	public void setRule(RuleService rule) {
		this.rule = rule;
	}


	public QueueMapping getQueueMap() {
		return queueMap;
	}

	public void setQueueMap(QueueMapping queueMap) {
		this.queueMap = queueMap;
	}
	
	public int begin(String tblName){
		this.tableName = tblName;
		this.dataMap = new HashMap<String,DataPack>();
		this.count = 0;
		return 0;
	}

	public int addRow(List<String> row){
		String actualTableName = calcActualTableName(row);
		DataPack dp = dataMap.get(actualTableName);
		if(dp == null){
			StringBuilder sb = getStringBuilder();
			dp = new DataPack(tableName,actualTableName,sb);
			dataMap.put(actualTableName, dp);
		}
		dp.addRow(row);
		this.count++;
		return count;
	}
	
	private StringBuilder getStringBuilder() {
		 return new StringBuilder(IBLConfig.blockSize);
	}

	public String calcActualTableName(List<String> row){		
		return rule.calcActualTableName(tableName,row);
	}
	
	public int commit() throws SQLException, UnsupportedEncodingException{ 
		for(Map.Entry<String,DataPack> entry : dataMap.entrySet()){
			DataPack dp = entry.getValue();
			dp.setBuf(dp.getData().toString().getBytes(IBLConfig.charset));
			//logger.info("buf size: "+dp.getBuf().length);
			dp.setData(null);
			BlockingQueue<DataPack> bqueue = queueMap.getQueueByActualTableName(dp.getActualTableName());
			if(bqueue == null){
				throw new SQLException("ActualTableName does not exist!!!");
			}
			try {
				bqueue.put(dp);
			} catch (InterruptedException e) {
				logger.info("InterruptedException!",e);
			}
		}
		return this.count;
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
