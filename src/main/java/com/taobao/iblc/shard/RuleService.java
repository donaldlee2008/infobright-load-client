package com.taobao.iblc.shard;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.api.Router;
import com.taobao.iblc.conf.IBLConfig;
import com.taobao.tddl.rule.le.TddlRule;
import com.taobao.tddl.rule.le.bean.TargetDatabase;

public class RuleService {
	protected final static Logger logger = LoggerFactory.getLogger(RuleService.class);
	private static TddlRule rule = null;
	private static String ruleFilePath = null;
	private static boolean useRuleEngine = false;
	private static Router routerRule  = null;
	private static String routerClass  = null;
	
	private static Map<String, ColumnInfo> shardInfo = IBLConfig.shardInfo;

	// private static TddlRouteRule route = null;
	public void init() {
		if(ruleFilePath != null){
		    rule = new TddlRule();
		    rule.setAppRuleFile(ruleFilePath);
		    // rule.setAppName(appName);
		    rule.init();
		}else{
			routerRule = getRouter(routerClass);
		}
	}

//	public String calcActualTableName(String logicTableName, List<String> row) {
//		if (useRuleEngine) {
//			String conditionStr = buildConditionStr(logicTableName, row);
//			// System.out.printf("ConditionStr:%s\n",conditionStr);
//			List<TargetDatabase> list = rule
//					.route(logicTableName, conditionStr);
//			TargetDatabase tdb = list.get(0);
//
//			return tdb.getDbIndex() + "." + tdb.getTableNames().get(0);
//		}else{
//			//return logicTableName+"_"+ Long.parseLong(row.get(2))%2+"_"+Integer.parseInt(row.get(4))%5;
//			return routerRule.calcActualTableName(logicTableName, row);
//		}
//			
//	}
	
	public String calcActualTableName(String logicTableName, String[] row) {
		if (useRuleEngine) {
			return null;
//			String conditionStr = buildConditionStr(logicTableName, row);
//			// System.out.printf("ConditionStr:%s\n",conditionStr);
//			List<TargetDatabase> list = rule
//					.route(logicTableName, conditionStr);
//			TargetDatabase tdb = list.get(0);
//
//			return tdb.getDbIndex() + "." + tdb.getTableNames().get(0);
		}else{
			//return logicTableName+"_"+ Long.parseLong(row.get(2))%2+"_"+Integer.parseInt(row.get(4))%5;
			return routerRule.calcActualTableName(logicTableName, row);
		}
			
	}

	private String buildConditionStr(String logicTableName, List<String> row) {
		String conditionStr = "";
		// Set<String> shardColumns = rule.getTableShardColumn(logicTableName);
		int pos = 0;
		String type = null;
		int i = 0;
		for (Map.Entry<String, ColumnInfo> entry : shardInfo.entrySet()) {
			String column = entry.getKey();
			pos = entry.getValue().getPos();
			type = entry.getValue().getType();
			conditionStr = conditionStr + column + " in (" + row.get(pos)
					+ "):" + type;

			if (i < shardInfo.size() - 1)
				conditionStr = conditionStr + ";";
			i++;
		}
		return conditionStr;
	}
	
	public Router getRouter(String routeClass) {
        Class<?> clazz=null;
        try{
        	clazz=Class.forName(routeClass);
        }catch (Exception e) {
        	logger.info("",e);
        }
        Router router=null;
        try {
        	router=(Router)clazz.newInstance();
        } catch (InstantiationException e) {
            logger.info("",e);
        } catch (IllegalAccessException e) {
        	logger.info("",e);
        }
        return router;
	}

	public boolean isUseRuleEngine() {
		return useRuleEngine;
	}

	public void setUseRuleEngine(boolean useRuleEngine) {
		this.useRuleEngine = useRuleEngine;
	}

	public static String getRuleFilePath() {
		return ruleFilePath;
	}

	public static void setRuleFilePath(String ruleFilePath) {
		RuleService.ruleFilePath = ruleFilePath;
	}	

	public static Router getRouterRule() {
		return routerRule;
	}

	public static void setRouterRule(Router routerRule) {
		RuleService.routerRule = routerRule;
	}

	public static String getRouterClass() {
		return routerClass;
	}

	public static void setRouterClass(String routerClass) {
		RuleService.routerClass = routerClass;
	}

	public static void main(String args[]) {
//		RuleService rservice = new RuleService();
//		rservice.init("hsflog-rule.xml");
//		// HsfLog hsfLog = HsfLog.parseHsfLog(s1);
//		List<String> row = new ArrayList<String>();
//		for (int i = 0; i <= 2; i++) {
//			row.add("0a60322613798620000257797");
//		}
//		row.add("2013-10-23 10:24:57");
//		System.out.printf(rservice.calcActualTableName("hsflog", row));
	}

}
