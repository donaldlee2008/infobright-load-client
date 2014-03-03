package com.taobao.iblc.api;


public interface Router {
	//public String calcActualTableName(String logicTableName, List<String> row);
	public String calcActualTableName(String logicTableName, String[] row);

}
