package com.taobao.iblc.hsflogtest;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;

public class HsfLog {
	private String cip;
	private String traceId;
	//private long time;
	private String time;
	private int time_days;
	private int time_ms;
	private int rpcType;
	private String rpcId;
	private String serviceName;
	private String methodName;
	private String sip;
	private String span;
	private String resultCode;
	private int requestSize;
	private int responseSize;
	private String appendMsg;

	public HsfLog(String cip, String traceId, long time, int rpcType,
			String rpcId, String serviceName, String methodName, String sip,
			String span, String resultCode, int requestSize, int responseSize,
			String appendMsg) {
		this.cip = cip;
		this.traceId = traceId;
		this.time = getDateTimeString(time);
		this.time_days = (int)(time/86400000);
		this.time_ms = (int)(time%86400000);
		this.rpcType = rpcType;
		this.rpcId = rpcId;
		this.serviceName = serviceName;
		this.methodName = methodName;
		this.sip = sip;
		this.span = span;
		this.resultCode = resultCode;
		this.requestSize = requestSize;
		this.responseSize = responseSize;
		this.appendMsg = appendMsg;
		
	}
	
	public static HsfLog parseHsfLog(String a){
		//System.out.println(tmp.length);
		HsfLog hsfLog;
		try {
			String[] array = StringUtils.split(a.toString(), "|");
			int requestsize = 0;
			int responsesize = 0;
			requestsize = Integer.parseInt(array[9]);


			responsesize = Integer.parseInt(array[10]);

			String[] tmp = array[0].split("\t");
			hsfLog = new HsfLog(tmp[0], tmp[1], Long.parseLong(array[1]),
					Integer.parseInt(array[2]), array[3], array[4], array[5],
					array[6], array[7], array[8], requestsize, responsesize,
					array[11]);
		} catch (Exception e) { 
			e.printStackTrace();
			hsfLog = new HsfLog("0","0",0L,0,"0","0","0","0","0","0",0,0,"0");
		}
		return hsfLog;
	}
	
	public void printHsfLog(){
		System.out.println("cip: "+cip);
		System.out.println("traceId: "+traceId);
		System.out.println("time: "+time);
		System.out.println("time_days: "+time_days);
		System.out.println("time_ms: "+time_ms);
		System.out.println("rpcType: "+rpcType);
		System.out.println("rpcId: "+rpcId);
		System.out.println("serviceName: "+serviceName);
		System.out.println("methodName: "+methodName);
		System.out.println("sip: "+sip);
		System.out.println("span: "+span);
		System.out.println("resultCode: "+resultCode);
		System.out.println("requestSize: "+requestSize);
		System.out.println("responseSize: "+responseSize);
		System.out.println("appendMsg: "+appendMsg);
	}
	
	private String getDateTimeString(long time){
		java.sql.Date d = new java.sql.Date(time);
        SimpleDateFormat sdformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//24小时制  
        //sdformat.setTimeZone(TimeZone.getTimeZone("GMT"));
        String LgTime = sdformat.format(d);
        return LgTime;
	}
	
	
	public int getTime_days() {
		return time_days;
	}

	public void setTime_days(int time_days) {
		this.time_days = time_days;
	}

	public int getTime_ms() {
		return time_ms;
	}

	public void setTime_ms(int time_ms) {
		this.time_ms = time_ms;
	}

	public String getCip() {
		return cip;
	}

	public void setCip(String cip) {
		this.cip = cip;
	}

	public String getTraceId() {
		return traceId;
	}

	public void setTraceId(String traceId) {
		this.traceId = traceId;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public int getRpcType() {
		return rpcType;
	}

	public void setRpcType(int rpcType) {
		this.rpcType = rpcType;
	}

	public String getRpcId() {
		return rpcId;
	}

	public void setRpcId(String rpcId) {
		this.rpcId = rpcId;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}
	
	public String getSip() {
		return sip;
	}

	public void setSip(String sip) {
		this.sip = sip;
	}

	public String getSpan() {
		return span;
	}

	public void setSpan(String span) {
		this.span = span;
	}

	public String getResultCode() {
		return resultCode;
	}

	public void setResultCode(String resultCode) {
		this.resultCode = resultCode;
	}

	public int getRequestSize() {
		return requestSize;
	}

	public void setRequestSize(int requestSize) {
		this.requestSize = requestSize;
	}

	public int getResponseSize() {
		return responseSize;
	}

	public void setResponseSize(int responseSize) {
		this.responseSize = responseSize;
	}

	public String getAppendMsg() {
		return appendMsg;
	}

	public void setAppendMsg(String appendMsg) {
		this.appendMsg = appendMsg;
	}
	


	public static void main(String[] args) {
		String a = "172.23.235.187	0a60322613798620000257797|1379862000066|1|0.1.5.1.17223235187.2|10032:2.0.0|getCoinRule~S|172.23.175.92|[1, 2]|00|998|0|@i^T7a89bbad^R ";
    
		HsfLog hsfLog = HsfLog.parseHsfLog(a);
		hsfLog.printHsfLog();
		return;
	}

}
