package com.taobao.iblc.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.shard.ColumnInfo;


//system variables
public class IBLConfig {
	protected final static Logger logger = LoggerFactory.getLogger(IBLConfig.class);
	
	
	public static String dbIP =null;	
	public static String dbport =null;	
	public static String dbname =null;	
	public static String dbusername =null;
	public static String dbpassword =null;

	public static List<List<String>> tableList = null;
	public static Map<String,ColumnInfo> shardInfo = null;
	//public static Map<String,String> createSqlMap = new HashMap<String,String>();
	public static String routeClass = null;
	
	public static int interval = 10; //idle interval between load data. in ms.
	public static String fieldSep = "\001";
	public static String lineSep = "\002";
	

	public static String charset = "UTF-8";
	//public static String charset = "GBK";
	public static int blockSize = 3*1024*1024;
	public static int loadQueueSize = 32;
	public static int maxReadByteCount = 20*1024*1024;
	public static Boolean useCache = false;
	
//	static{
//		init("loaddata.properties");
//	}
	
	public static void init(String propertiesFile){
		InputStream is = IBLConfig.class.getClassLoader().getResourceAsStream(propertiesFile);   
		Properties p = new Properties();   
		try {   
		    p.load(is);    
		    is.close();
		}catch(IOException e) {
			logger.info("load property file failed!!!\n" + e.getMessage());   
		}   
 
		dbIP = p.getProperty("dbIP").trim();
		dbport = p.getProperty("dbport").trim();
		dbname = p.getProperty("dbname").trim();
		dbusername = p.getProperty("dbusername").trim();
		dbpassword = p.getProperty("dbpassword").trim();

		//tableList = getTableList(p);
		
		shardInfo = getShardInfo(p);
		routeClass = p.getProperty("routeRule").trim();
		
		
		//createSqlMap.put(p.getProperty("vtab"), p.getProperty("sql"));
		
		if(p.getProperty("interval") != null)
			interval = Integer.parseInt(p.getProperty("interval").trim());
		if(p.getProperty("blockSize") != null){
			blockSize = Integer.parseInt(p.getProperty("blockSize").trim());
		}
		if(p.getProperty("loadQueueSize") != null){
			loadQueueSize = Integer.parseInt(p.getProperty("loadQueueSize").trim());
		}
		if(p.getProperty("maxReadByteCount") != null){
			maxReadByteCount = Integer.parseInt(p.getProperty("maxReadByteCount").trim());
		}
		
		logger.info("****Config info****");
		logger.info("dbIP:"+dbIP);
		logger.info("dbport:"+dbport);
		logger.info("dbname:"+dbname);
		logger.info("dbusername:"+dbusername);
		logger.info("dbpassword:"+dbpassword);
		//logger.info("tableNames:"+tableList);
		logger.info("shardInfo:"+shardInfo);
		logger.info("routeClass:"+routeClass);
		logger.info("interval:"+interval);
		logger.info("blockSize:"+blockSize);
		logger.info("loadQueueSize:"+loadQueueSize);
		logger.info("maxReadByteCount:"+maxReadByteCount);
		logger.info("****Config info****\n");
	}
	
//	private static Map<String, String> getRouteMap(Properties p) {
//		Map<String,String> routeMap = new HashMap<String,String>();
//		String[] routeStr = p.getProperty("routeRules").split(";");
//		for(String str : routeStr){
//			String[] tmp = str.split(",");
//			routeMap.put(tmp[0], tmp[1]);
//		}
//		return routeMap;
//	}
	
	public static List<List<String>> getTableList(Properties p){
		List<List<String>> list = new ArrayList<List<String>>();
		String[] tableNames = p.getProperty("tableNames").trim().split(";");
		for(String str:tableNames){
			String[] tmp = str.trim().split(",");
			List<String> array =  Arrays.asList(tmp);
			list.add(array);
		}
		return list;
	}

	public static Map<String,ColumnInfo> getShardInfo(Properties p){
		Map<String,ColumnInfo> shardInfo = new HashMap<String,ColumnInfo>();
		String[] shardColumns = p.getProperty("shardColumns").split(",");
		for(String str:shardColumns){
			String[] tmp = str.split(":");
			shardInfo.put(tmp[0], new ColumnInfo(Integer.parseInt(tmp[1]),tmp[2]));
		}
		return shardInfo;
	}
	
	public static void main(String args[]) {
		IBLConfig.init("loaddata.properties");
	}

}
