package com.taobao.iblc.hsflogtest;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.taobao.iblc.BatchHandler;
import com.taobao.iblc.IBLClient;
import com.taobao.iblc.RuleService;


public class HsfLogRunnable implements Runnable {
	private String filePath = null;
	private int batchSize;
	private IBLClient client = null;
	
	public HsfLogRunnable(String filePath,int batchSize,IBLClient client){
		this.filePath = filePath;
		this.batchSize = batchSize;
		this.client = client;
		
	}

	public List<String> buildRow(HsfLog hsfLog){
		List<String> row = new ArrayList<String>();
		row.add(hsfLog.getCip());
		row.add(hsfLog.getTraceId());
		row.add(Long.toString(convertToNumber(hsfLog.getTraceId())));
		row.add(hsfLog.getTime());
		row.add(Integer.toString(hsfLog.getTime_days()));
		row.add(Integer.toString(hsfLog.getTime_ms()));
		row.add(Integer.toString(hsfLog.getRpcType()));
		row.add(hsfLog.getRpcId());
		row.add(hsfLog.getServiceName());
		row.add(hsfLog.getMethodName());
		row.add(hsfLog.getSip());
		row.add(hsfLog.getSpan());
		row.add(hsfLog.getResultCode());
		row.add(Integer.toString(hsfLog.getRequestSize()));
		row.add(Integer.toString(hsfLog.getResponseSize()));
		row.add(hsfLog.getAppendMsg());
		return row;
	}
	
	public void run() {
		try {
			FileReader reader = new FileReader(filePath);
			BufferedReader br = new BufferedReader(reader);

			BatchHandler batch = client.getHandler();
			batch.begin("hsflog");
			int i = 0;//the num of read record 
			String s1 = null;
			while ((s1 = br.readLine()) != null) {	
				//System.out.println(s1);
				HsfLog hsfLog = HsfLog.parseHsfLog(s1);
				List<String> row = buildRow(hsfLog);
				batch.addRow(row);
				i++;
				if (i % batchSize == 0) {
					int t = 0;
					try {
						t = batch.commit();
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("commit "+ t +" log row!!!");
					batch.begin("hsflog");
				}
			}
			System.out.println("load file end!!! total load "+ i +" log row");
			br.close();
			reader.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/**
	 * equals mysql func:
	 * conv(substr(md5('?'),1,14),16,10)
	 * @param a string
	 * @return 
	 */
	public long convertToNumber(String values){
		java.security.MessageDigest md = null;
		try {
			md = java.security.MessageDigest  
			        .getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}  
		md.update(values.getBytes());
		byte tmp[] = md.digest();
		int index = 0;
		return (  
                (((long) tmp[index + 0] & 0xff) << 48)  
              | (((long) tmp[index + 1] & 0xff) << 40)  
              | (((long) tmp[index + 2] & 0xff) << 32)  
              | (((long) tmp[index + 3] & 0xff) << 24)  
              | (((long) tmp[index + 4] & 0xff) << 16)  
              | (((long) tmp[index + 5] & 0xff) << 8) 
              | (((long) tmp[index + 6] & 0xff) << 0)
              );
	}
	
	public static void main(String args[]) {
		try {
			IBLClient client = new IBLClient();
			//client.setRuleFilePath("hsflog-rule.xml");
			client.setConfigFilePath("loaddata.properties");
			client.init();
			
			RuleService ruleS = client.getRule();
			
			HsfLogRunnable hsf = new HsfLogRunnable(null,0,null);
           
			FileReader reader = new FileReader("/home/zjy/text.file");
			BufferedReader br = new BufferedReader(reader);
			
			List<List<String>> list = new ArrayList<List<String>>();
			
            int batchSize = 1000000;
			int i = 0;//the num of read record 
			String s1 = null;
			while ((s1 = br.readLine()) != null) {	
				//System.out.println(s1);
				HsfLog hsfLog = HsfLog.parseHsfLog(s1);
				List<String> row = hsf.buildRow(hsfLog);
				list.add(row);
				i++;
				if (i == batchSize) {
					break;
				}
			}
			
			
			long begintime=System.currentTimeMillis();
			for(List<String> row : list){
				long num = hsf.convertToNumber(row.get(1));
			}
			long endtime=System.currentTimeMillis();
			double costTimeSec = (endtime - begintime)/1000.00;
			System.out.println("convert total "+list.size()+" cost time in second : "+ costTimeSec);
			
			
			begintime=System.currentTimeMillis();
			for(List<String> row : list){				
				ruleS.calcActualTableName("hsflog", row);
			}
			endtime=System.currentTimeMillis();
			costTimeSec = (endtime - begintime)/1000.00;
			System.out.println("rule calc total "+list.size()+" cost time in second : "+ costTimeSec);
			
			br.close();
			reader.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
//	public static void main(String args[]) {
//		try {
//			
//			HsfLogRunnable hsf = new HsfLogRunnable(null,0,null);
//			FileReader reader = new FileReader("/home/zjy/text.file");
//			BufferedReader br = new BufferedReader(reader);
//            int batchSize = 1000000;
//			String[] traceIdArray = new String[batchSize];
//			int i = 0;//the num of read record 
//			String s1 = null;
//			while ((s1 = br.readLine()) != null) {	
//				//System.out.println(s1);
//				HsfLog hsfLog = HsfLog.parseHsfLog(s1);
//				//List<String> row = buildRow(hsfLog);
//				traceIdArray[i] = hsfLog.getTraceId();
//				i++;
//				if (i == batchSize) {
//					break;
//				}
//			}
//
//			int[] counts = new int[4];
//			for(i = 0;i<4;i++)
//				counts[i] = 0;
//			long begintime=System.currentTimeMillis();
//			for(i = 0;i<batchSize;i++){
//				long num = hsf.convertToNumber(traceIdArray[i]);
//				int t = (int)(num%4);				
//				counts[t]++;
//			}
//			long endtime=System.currentTimeMillis();
//			double costTimeSec = (endtime - begintime)/1000.00;
//			double speed = batchSize/costTimeSec;
//			System.out.println("total cost time: "+ costTimeSec +"sec");
//			System.out.println("speed: "+ speed+"r/s");
//			for(i=0;i<4;i++){
//				System.out.println("count mod "+ i +" : "+counts[i]);
//			}
//			//System.out.println("load file end!!! total load "+ i +" log row");
//			
//			br.close();
//			reader.close();
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}		
//	}


}
