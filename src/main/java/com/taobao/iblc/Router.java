package com.taobao.iblc;

import java.util.List;

public interface Router {
	public String calcActualTableName(String logicTableName, List<String> row);

}
