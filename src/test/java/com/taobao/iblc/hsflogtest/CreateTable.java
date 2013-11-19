package com.taobao.iblc.hsflogtest;

import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.taobao.iblc.DButil;
import com.taobao.iblc.IBLConfig;


public class CreateTable {
	
	protected final static Logger logger = LoggerFactory
			.getLogger(CreateTable.class);
	
	private static String database = "eaglelog";
	private static String vtab = "hsflog";
	
	private static Connection ibconn = null;
	
	public static void main(String args[]) {
		IBLConfig.init("loaddata.properties");
		String sql = "drop database "+database;
		checkConnection();
		try {
			DButil.executeUpdate(ibconn, sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		sql = "create database "+database;
		try {
			DButil.executeUpdate(ibconn, sql);
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		int i,j;
		for(i=0;i<2;i++){
			for(j=0;j<5;j++){
				String actualTblName = database +"."+ vtab+"_"+i+"_"+j;
				sql = makeCreateTableSql(vtab, actualTblName);
				try {
					DButil.executeUpdate(ibconn, sql);
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
		}
		
	}

	private static void checkConnection() {
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
	
	private static String makeCreateTableSql(String vtab, String actualTableName) {
		String sql = IBLConfig.createSqlMap.get(vtab).replace("?",
				actualTableName);
		return sql;
	}
	
}
