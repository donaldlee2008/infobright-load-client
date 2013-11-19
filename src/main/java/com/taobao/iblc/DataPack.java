package com.taobao.iblc;

import java.util.List;

public class DataPack {
	private String vtab = null;//logic table name.
	private String actualTableName = null;//dbname.tblname
	private StringBuilder data = null;//new StringBuilder(); 
	private byte buf[] = null;
	
	public DataPack(String vtab,String actualTableName,StringBuilder data){
		this.vtab = vtab;
		this.data = data;
		this.actualTableName = actualTableName;
	}
	
	public void addRow(List<String> row){
		int i;
		for(i=0;i<row.size()-1;i++){
			data.append(row.get(i));
			data.append(IBLConfig.fieldSep);
		}
		data.append(row.get(i));
		data.append(IBLConfig.lineSep);
	}
	
	public String getVtab() {
		return vtab;
	}

	public void setVtab(String vtab) {
		this.vtab = vtab;
	}

	public String getActualTableName() {
		return actualTableName;
	}
	
	public void setActualTableName(String actualTableName) {
		this.actualTableName = actualTableName;
	}
	
	public StringBuilder getData() {
		return data;
	}
	
	public void setData(StringBuilder data) {
		this.data = data;
	}

	public byte[] getBuf() {
		return buf;
	}

	public void setBuf(byte[] buf) {
		this.buf = buf;
	}
	
	
}
