package com.taobao.iblc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.conf.IBLConfig;
import com.taobao.iblc.pojo.DataPack;

public class IBLoaderNew implements Runnable{
	protected final static Logger logger = LoggerFactory
			.getLogger(IBLoaderNew.class);

	private String id = null;

	private Connection ibconn = null;

	private BlockingQueue<DataPack> queue = null;	

	//private static String charset = IBLConfig.charset;
	private long interval = IBLConfig.interval;// idle between load
	
	private int finished = 0;

	public IBLoaderNew(String id, BlockingQueue<DataPack> q) {
		this.id = id;
		this.queue = q;
	}

	private void stop() {
		logger.info("loader stopping!!!");
		try {
			if (ibconn != null)
				ibconn.close();
		} catch (SQLException e) {
			logger.error("close jdbc connection error!!!", e);
		} catch (Exception e) {
			logger.error("close jdbc connection error!!!", e);
		}finally{
			
		}
	}
	
	public void run() {
		// this.saveTime = System.currentTimeMillis();
		while (true) {
			IBLoaderInputStream is = null;
			// IBLoaderInputStreamNew is = null;
			com.mysql.jdbc.PreparedStatement mysqlStatement = null;
				
			if(finished == 1 && queue.size() == 0 ){
				stop();
				break;
			}
			try {	
				is = new IBLoaderInputStream(queue);
				// is = new IBLoaderInputStreamNew(queue);
				is.init();
				// obtain PreparedStatement for load data into infobright.
				mysqlStatement = obtainLoadMysqlStatement(is
						.getActualTableName());

				DButil.loadData(mysqlStatement, is);
				
//				if(queue.size()>=IBLConfig.loadQueueSize/5)
//				    logger.info("current Queue size: " + queue.size());

			} catch (InterruptedException e) {
				logger.info("ibloader stop when take task from queue",e);				
				finished = 1;
			} catch (SQLException e) {
				logger.error("load failed!!!", e);
			} catch (Exception e) {
				logger.error("load failed!!!", e);
			}finally {
				try {
					if (null != is)
						is.close();
					if (null != mysqlStatement)
						mysqlStatement.close();
				} catch (IOException e) {
					logger.info("close inputstream error!!!", e);
				} catch (SQLException e) {
					logger.info("close mysqlStatement error!!!", e);
				} catch (Exception e) {
					logger.error("close inputstream or mysqlStatement error!!!", e);
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
