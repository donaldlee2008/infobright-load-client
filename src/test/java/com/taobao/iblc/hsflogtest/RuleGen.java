package com.taobao.iblc.hsflogtest;

import java.util.List;

import com.taobao.iblc.Router;

public class RuleGen implements Router {
	
	public String calcActualTableName(String logicTableName, List<String> row) {
		return "eaglelog."+logicTableName+"_"+ Long.parseLong(row.get(2))%2+"_"+Integer.parseInt(row.get(4))%5;
	}

	
	public static void main(String[] args) {

	}
}
