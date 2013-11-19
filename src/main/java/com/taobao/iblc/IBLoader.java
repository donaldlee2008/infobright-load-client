package com.taobao.iblc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IBLoader implements Runnable {
	protected final static Logger logger = LoggerFactory
			.getLogger(IBLoader.class);

	private String id = null;

	private Connection ibconn = null;

	private BlockingQueue<DataPack> queue = null;

	private static String charset = IBLConfig.charset;
	private long interval = IBLConfig.interval;// idle between load

	private void stop() {
		logger.info("loader stopping!!!");
		try {
			if (ibconn != null)
				ibconn.close();
		} catch (SQLException e) {
			logger.info("close jdbc statment or connection error!!!", e);
		}
	}

	public IBLoader(String id, BlockingQueue<DataPack> queue) {
		this.id = id;
		this.queue = queue;
	}

	public void run() {
		while (true) {
			// logger.info("new loop!!!");
			InputStream is = null;
			DataPack dp = null;
			com.mysql.jdbc.PreparedStatement mysqlStatement = null;
			try {
				dp = queue.take();
				// logger.info("get a datapack to load!!!");

				// check if need create new table.
				checkNeedNewTable(dp);
				is = new ByteArrayInputStream(dp.getData().toString()
						.getBytes(charset));
				// obtain PreparedStatement for load data into infobright.
				mysqlStatement = obtainLoadMysqlStatement(dp
						.getActualTableName());
				// logger.info("start load!!!");
				DButil.loadData(mysqlStatement, is);

				// sleep after load.
				Thread.sleep(this.interval);

			} catch (InterruptedException e) {
				logger.info("ibloader stop when take task from queue or sleep between loading");
				stop();
			} catch (SQLException e) {
				logger.error("load failed！！", e);
				// logger.error(dp.getData().toString().replace(IBLConfig.lineSep,'\n'));
				// InfoLoaderUtil.processFatal("load failed！！",e);
			} catch (UnsupportedEncodingException e) {
				logger.error(
						"Unsupported Encoding when generate Stream for loader！！",
						e);
				// InfoLoaderUtil.processFatal("Unsupported Encoding when generate Stream for loader！！",e);
			} finally {
				try {
					if (null != is)
						is.close();
					if (null != mysqlStatement)
						mysqlStatement.close();
				} catch (IOException e) {
					logger.info("close inputstream error！！", e);
				} catch (SQLException e) {
					logger.info("close mysqlStatement error！！", e);
				}
			}

		}

	}

	private void checkNeedNewTable(DataPack dp) {
		boolean flag = true;
		checkConnection();
		try {
			flag = DButil.existTable(ibconn, dp.getActualTableName());
		} catch (SQLException e) {
			logger.info(
					"call DButil.existTable failed !!! table name : "
							+ dp.getActualTableName(), e);
			// InfoLoaderUtil.processFatal("call DButil.existTable failed !!!",e);
		}
		if (flag == false) {// need create new table
			// create new table
			String sql = makeCreateTableSql(dp.getVtab(),
					dp.getActualTableName());
			checkConnection();
			try {
				DButil.executeUpdate(ibconn, sql);
			} catch (SQLException e1) {
				// InfoLoaderUtil.processFatal("create new table failed!!!",e1);
			}

		}
	}


	private String makeCreateTableSql(String vtab, String actualTableName) {
		String sql = IBLConfig.createSqlMap.get(vtab).replace("?",
				actualTableName);
		return sql;
	}

	private String makeLoadDataSql(String tblName) {
		String loadDataSql = "LOAD DATA LOCAL INFILE 'jiyuan.csv' INTO TABLE "
				+ tblName +
				// " CHARACTER SET "+"utf8" +
				" FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\002' ";
		return loadDataSql;
	}

	private com.mysql.jdbc.PreparedStatement obtainLoadMysqlStatement(
			String tblName) {

		checkConnection();
		String loadDataSql = makeLoadDataSql(tblName);
		com.mysql.jdbc.PreparedStatement mysqlStatement = DButil
				.getLoadMysqlStatement(ibconn, loadDataSql);

		return mysqlStatement;
	}

	private void checkConnection() {
		if (ibconn == null) {
			try {
				ibconn = DButil.getConnection(IBLConfig.dbIP, IBLConfig.dbport,
						IBLConfig.dbname, IBLConfig.dbusername,
						IBLConfig.dbpassword);
			} catch (ClassNotFoundException e) {
				logger.error("establish connection to infobright failed！！", e);
				// InfoLoaderUtil.processFatal("establish connection to infobright failed！！",e);
			} catch (SQLException e) {
				logger.error("establish connection to infobright failed！！", e);
				// InfoLoaderUtil.processFatal("establish connection to infobright failed！！",e);
			}
		}
	}

	public Connection getIbconn() {
		return ibconn;
	}

	public void setIbconn(Connection ibconn) {
		this.ibconn = ibconn;
	}

	public BlockingQueue<DataPack> getQueue() {
		return queue;
	}

	public void setQueue(BlockingQueue<DataPack> queue) {
		this.queue = queue;
	}

	public String getId() {
		return id;
	}

	public static void main(String args[]) {
		IBLoader ibl = new IBLoader(null, null);
		// String sql =
		// ibl.makeCreateTableSql("notify_trace_group_0.notify_msg_0_20131009");
		// System.out.println(sql);
	}
}
