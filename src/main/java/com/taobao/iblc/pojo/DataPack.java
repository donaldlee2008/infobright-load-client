package com.taobao.iblc.pojo;

import java.io.UnsupportedEncodingException;
import java.util.LinkedList;


import com.taobao.iblc.conf.IBLConfig;

public class DataPack {
	private String vtab = null;//logic table name.
	private String actualTableName = null;//dbname.tblname
	private StringBuilder data = null;//new StringBuilder(); 
	private byte buf[] = null;
	
	private LinkedList<DataBlock> dataArea = new LinkedList<DataBlock>();


	private int BUF_SIZE = 1024*1024;
	private DataBlock curBlock;
	
	public DataPack(String vtab,String actualTableName,StringBuilder data){
		this.vtab = vtab;
		this.data = data;
		this.actualTableName = actualTableName;
	}
	
	public DataPack(String vtab,String actualTableName){
		this.vtab = vtab;
		this.actualTableName = actualTableName;
		curBlock = new DataBlock(BUF_SIZE);
		dataArea.add(curBlock);
	}
	
//	public void addRow(List<String> row) { 
//		int i;
//		for(i=0;i<row.size()-1;i++){
//			data.append(row.get(i));
//			data.append(IBLConfig.fieldSep);
//		}
//		data.append(row.get(i));
//		data.append(IBLConfig.lineSep);
////		int i=0;
////		for(i=0;i<row.size()-1;i++){
////			saveBytes(row.get(i).getBytes(IBLConfig.charset));
////			saveBytes(IBLConfig.fieldSep.getBytes(IBLConfig.charset));			
////		}
////		saveBytes(row.get(i).getBytes(IBLConfig.charset));
////		saveBytes(IBLConfig.lineSep.getBytes(IBLConfig.charset));
//	}
	
	public void addRow(String[] row) { 
		int i;
		for(i=0;i<row.length-1;i++){
			data.append(row[i]);
			data.append(IBLConfig.fieldSep);
		}
		data.append(row[i]);
		data.append(IBLConfig.lineSep);
//		int i=0;
//		for(i=0;i<row.size()-1;i++){
//			saveBytes(row.get(i).getBytes(IBLConfig.charset));
//			saveBytes(IBLConfig.fieldSep.getBytes(IBLConfig.charset));			
//		}
//		saveBytes(row.get(i).getBytes(IBLConfig.charset));
//		saveBytes(IBLConfig.lineSep.getBytes(IBLConfig.charset));
	}
	
	private void saveBytes(byte[] buf) {
		if(curBlock.getLen() + buf.length > BUF_SIZE){			
			curBlock = new DataBlock(BUF_SIZE);
			dataArea.add(curBlock);
		}
		System.arraycopy(buf, 0, curBlock.getData(), curBlock.getLen(), buf.length);
		curBlock.setLen(curBlock.getLen()+buf.length);
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
	
	public LinkedList<DataBlock> getDataArea() {
		return dataArea;
	}

	public void setDataArea(LinkedList<DataBlock> dataArea) {
		this.dataArea = dataArea;
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
	
	public static void main(String args[]) {
		try {
			byte[] bytes = "\001".getBytes("UTF-8");
			System.out.println(bytes.length);
			System.out.println(bytes);
			LinkedList<String> lst = new LinkedList<String>();
			
			lst.add("zhao");
			lst.add("ji");
			lst.add("yuan");
			System.out.println(lst.remove(0));
			System.out.println(lst.remove(0));
			System.out.println(lst.remove(0));
			System.out.println(lst.size());
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
