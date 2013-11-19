package com.taobao.iblc;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class IBLClient {
	private static String ruleFilePath = null;
	private static String appName = null;

	private static String configFilePath = null;

	private static QueueMapping queueMap = null;
	private static RuleService rule = null;
	//private static List<IBLoader> ibloaderList = new ArrayList<IBLoader>();
	private static List<Thread> loaderTL = new ArrayList<Thread>();
	private static List<IBLoaderNew> ibloaderListNew = new ArrayList<IBLoaderNew>();

	static {

	}

	public void init() {
		IBLConfig.init(configFilePath);
		// queueMap init.
		queueMap = new QueueMapping();
//		for (String id : IBLConfig.tableList) {
//			BlockingQueue<DataPack> bq = new ArrayBlockingQueue<DataPack>(8);
//			IBLoader ibl = new IBLoader(id, bq);
//			queueMap.put(id, bq);
//			ibloaderList.add(ibl);
//			Thread t = new Thread(ibl, "IBLoader-" + ibl.getId());
//			loaderTL.add(t);
//			t.start();
//		}
		
		for (String id : IBLConfig.tableList) {
			BlockingQueue<DataPack> bq = new ArrayBlockingQueue<DataPack>(IBLConfig.loadQueueSize);
			IBLoaderNew ibl = new IBLoaderNew(id, bq);
			queueMap.put(id, bq);
			ibloaderListNew.add(ibl);
			Thread t = new Thread(ibl, "IBLoader-" + ibl.getId());
			loaderTL.add(t);
			t.start();
		}
		
		//init cache
//		if(IBLConfig.useCache){
//			StringBuilderCache.init(IBLConfig.loadQueueSize*IBLConfig.tableList.size()+1, IBLConfig.blockSize);
//		}
		
		rule = new RuleService();
		if(ruleFilePath != null){
		    rule.setRuleFilePath(ruleFilePath);		    
		    rule.setUseRuleEngine(true);
		}else{
            //rule.setRouterRule(getRouter());
			rule.setRouterClass(IBLConfig.routeClass);
			rule.setUseRuleEngine(false);
		}
		rule.init();
	}

	
	public void close() {
		for (Thread t : loaderTL) {
			t.interrupt();
		}
	}

	public String getRuleFilePath() {
		return ruleFilePath;
	}

	public void setRuleFilePath(String ruleFilePath) {
		this.ruleFilePath = ruleFilePath;
	}

	public BatchHandler getHandler() {
		BatchHandler data = new BatchHandler();
		// data.setTableName(tblName);
		data.setQueueMap(queueMap);
		data.setRule(rule);
		return data;
	}
	
	
	public String getConfigFilePath() {
		return configFilePath;
	}

	public void setConfigFilePath(String configFilePath) {
		IBLClient.configFilePath = configFilePath;
	}

	public static RuleService getRule() {
		return rule;
	}

	public static void setRule(RuleService rule) {
		IBLClient.rule = rule;
	}

	// public int addRow(BatchData data,List<String> row){
	// data.addRow(row);
	// return 0;
	// }
	//
	// public int commitBatch(BatchData data){
	// data.commit();
	// return 0;
	// }
}
