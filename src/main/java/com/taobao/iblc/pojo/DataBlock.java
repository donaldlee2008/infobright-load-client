package com.taobao.iblc.pojo;

public class DataBlock {
	
	private byte[] data;
	private int len;
	
	public DataBlock(int capacity) {
		super();
		this.data = new byte[capacity];
		this.len = 0;
	}
	public byte[] getData() {
		return data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	public int getLen() {
		return len;
	}
	public void setLen(int len) {
		this.len = len;
	}
	
	
}
