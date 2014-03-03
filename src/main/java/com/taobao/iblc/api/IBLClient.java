package com.taobao.iblc.api;


import com.taobao.iblc.TaskGroup;
import com.taobao.iblc.conf.IBLConfig;
import com.taobao.iblc.shard.RuleService;

public class IBLClient {
	private static String ruleFilePath = null;
	private static String appName = null;

	private static String configFilePath = null;

	private static TaskGroup queueMap = null;
	private static RuleService rule = null;
	

	static {

	}

	public void init() {
		IBLConfig.init(configFilePath);
		// queueMap init.
		queueMap = new TaskGroup();

//		int i = 0;
//		for (i = 0;i<IBLConfig.tableList.size();i++) {
//			
//			BlockingQueue<DataPack> bq = new ArrayBlockingQueue<DataPack>(IBLConfig.loadQueueSize);
//			for(String tbl:IBLConfig.tableList.get(i)){
//			    queueMap.put(tbl, bq);			    
//			}
//			
//			String id = "loader "+i;	
//					
//			IBLoaderNew ibl = new IBLoaderNew(id, bq);
//						
//			ibloaderListNew.add(ibl);
//			Thread t = new Thread(ibl, "IBLoader-" + ibl.getId());
//			loaderTL.add(t);
//			t.start();
//		}		
		
		//rule init.
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
		queueMap.close(); 
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
