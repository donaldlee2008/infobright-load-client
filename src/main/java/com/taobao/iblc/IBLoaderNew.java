package com.taobao.iblc;


import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IBLoaderNew implements Runnable{
	protected final static Logger logger = LoggerFactory
			.getLogger(IBLoader.class);

	private String id = null;

	private Connection ibconn = null;

	private BlockingQueue<DataPack> queue = null;
	
//	private long saveTime = 0;
//	private long printInterval = 8000;

	//private static String charset = IBLConfig.charset;
	private long interval = IBLConfig.interval;// idle between load

	public IBLoaderNew(String id2, BlockingQueue<DataPack> bq) {
		this.id = id2;
		this.queue = bq;
	}

	private void stop() {
		logger.info("loader stopping!!!");
		try {
			if (ibconn != null)
				ibconn.close();
		} catch (SQLException e) {
			logger.info("close jdbc statment or connection error!!!", e);
		}
	}
	
	public void run() {
		//this.saveTime = System.currentTimeMillis();
		while (true) {
			// logger.info("new loop!!!");
//			if(System.currentTimeMillis() - this.saveTime > this.printInterval){
//				logger.info("Queue size: "+this.queue.size());
//				this.saveTime = System.currentTimeMillis();
//			}
			
			IBLoaderInputStream is = null;
			//DataPack dp = null;
			com.mysql.jdbc.PreparedStatement mysqlStatement = null;
			try {
                
				is = new IBLoaderInputStream(queue);
				is.init();
				// obtain PreparedStatement for load data into infobright.
				mysqlStatement = obtainLoadMysqlStatement(is.getActualTableName());
				// logger.info("start load!!!");
				DButil.loadData(mysqlStatement, is);
				logger.info("current Queue size: "+this.queue.size());
				// sleep after load.
				Thread.sleep(this.interval);

			} catch (InterruptedException e) {
				logger.info("ibloader stop when take task from queue or sleep between loading");
				stop();
			} catch (SQLException e) {
				logger.error("load failed！！", e);
				// logger.error(dp.getData().toString().replace(IBLConfig.lineSep,'\n'));
				// InfoLoaderUtil.processFatal("load failed！！",e);
			} finally {
				try {
					if (null != is)
						is.close();
					if (null != mysqlStatement)
						mysqlStatement.close();
				} catch (IOException e) {
					logger.info("close inputstream error!!!", e);
				} catch (SQLException e) {
					logger.info("close mysqlStatement error!!!", e);
				}
			}

		}
	}
	
	private com.mysql.jdbc.PreparedStatement obtainLoadMysqlStatement(
			String tblName) {

		checkConnection();
		String loadDataSql = makeLoadDataSql(tblName);
		com.mysql.jdbc.PreparedStatement mysqlStatement = DButil
				.getLoadMysqlStatement(ibconn, loadDataSql);

		return mysqlStatement;
	}
	
	private String makeLoadDataSql(String tblName) {
		String loadDataSql = "LOAD DATA LOCAL INFILE 'jiyuan.csv' INTO TABLE "
				+ tblName +
				// " CHARACTER SET "+"utf8" +
				" FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\002' ";
		return loadDataSql;
	}
	
	private void checkConnection() {
		if (ibconn == null) {
			try {
				ibconn = DButil.getConnection(IBLConfig.dbIP, IBLConfig.dbport,
						IBLConfig.dbname, IBLConfig.dbusername,
						IBLConfig.dbpassword);
			} catch (ClassNotFoundException e) {
				logger.error("establish connection to infobright failed!!!", e);
				// InfoLoaderUtil.processFatal("establish connection to infobright failed！！",e);
			} catch (SQLException e) {
				logger.error("establish connection to infobright failed!!!", e);
				// InfoLoaderUtil.processFatal("establish connection to infobright failed！！",e);
			}
		}
	}

	public String getId() {
		return id;
	}


}
