package com.taobao.iblc.shard;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import com.taobao.tddl.interact.bean.ComparativeMapChoicer;
import com.taobao.tddl.interact.bean.TargetDB;
import com.taobao.tddl.interact.sqljep.Comparative;
import com.taobao.tddl.rule.le.TddlRule;

public class MyComparativeMapChoicer implements ComparativeMapChoicer{
	protected final static Logger logger = LoggerFactory.getLogger(RuleService.class);
	
	private Map<String,Comparative> comparativeMap=new HashMap<String, Comparative>();

	
	public MyComparativeMapChoicer(Map<String, Comparative> comparativeMap) {
		super();
		this.comparativeMap = comparativeMap;
	}

	public Map<String, Comparative> getColumnsMap(List<Object> arguments,
			Set<String> partnationSet) {
		
		return this.comparativeMap;
	}

	public Comparative getColumnComparative(List<Object> arguments,
			String colName) {
		
		return this.comparativeMap.get(colName);
	}
	
	public static void main(String args[]) {
		TddlRule rule = new TddlRule();
		//rule.setAppRuleFile("tddl-rule-notify-new.xml");

		rule.setAppName("notify_trace_app_new");

		rule.init();
		Map<String,Comparative> comparativeMap=new HashMap<String, Comparative>();
		
		comparativeMap.put("message_id".toUpperCase(),new Comparative(3,"8FABA2621386CA2924E0451F44D598A8"));
		comparativeMap.put("gmt_create_days".toUpperCase(),new Comparative(3,15970));
		ComparativeMapChoicer choicer = new MyComparativeMapChoicer(comparativeMap);
		
		List<TargetDB> list = rule.routeWithTargetDbResult("notify_msg", choicer);
        if (list == null || list.isEmpty()) {
            throw new IllegalArgumentException("can't find target db. table is " + "notify_msg" + "." );
        }
        if (list.size() >= 2) {// 双版本，意味着切换中，目前暂时还是全部丢异常吧。
            throw new IllegalArgumentException("双版本切换中，目前不支持！！");
        } else if (list.size() == 1) {
        	TargetDB tdb = list.get(0);
        	System.out.println(tdb.getDbIndex() + "." + tdb.getTableNames());
        }
		
	}

}
