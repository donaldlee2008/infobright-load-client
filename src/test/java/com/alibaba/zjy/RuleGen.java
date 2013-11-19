package com.alibaba.zjy;

import java.util.List;

import com.taobao.iblc.Router;

public class RuleGen implements Router {
	
	public String calcActualTableName(String logicTableName, List<String> row) {
		return "eagleeye."+logicTableName+"_"+ Long.parseLong(row.get(1))%4+"_"+Integer.parseInt(row.get(3))%5;
	}

	
	public static void main(String[] args) {

	}
}
