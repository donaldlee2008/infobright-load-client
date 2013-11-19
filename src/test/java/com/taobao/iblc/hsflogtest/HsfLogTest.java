package com.taobao.iblc.hsflogtest;


import com.taobao.iblc.IBLClient;


public class HsfLogTest {
	private static String filePath = "/home/zjy/text.file";
	private static int batchSize = 10000;
	private static IBLClient client = null;

	
	public static void main(String args[]) {
		try {

			client = new IBLClient();
			//client.setRuleFilePath("hsflog-rule.xml");
			client.setConfigFilePath("loaddata.properties");
			client.init();
			
			HsfLogRunnable log0 = new HsfLogRunnable(filePath,batchSize,client);
			Thread t0 = new Thread(log0,"log0"); 
			t0.start();
//			HsfLogRunnable log1 = new HsfLogRunnable(filePath,batchSize,client);
//			Thread t1 = new Thread(log1,"log1"); 
//			t1.start();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
}
