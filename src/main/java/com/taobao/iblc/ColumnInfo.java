package com.taobao.iblc;

public class ColumnInfo {
	
	private int pos;
	private String type;
	
	public ColumnInfo(int pos,String type){
		this.pos = pos;
		this.type = type;
	}
	
	public int getPos() {
		return pos;
	}
	public void setPos(int pos) {
		this.pos = pos;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}

	public String toString(){
		return pos + ","+type;
	}
	
}
