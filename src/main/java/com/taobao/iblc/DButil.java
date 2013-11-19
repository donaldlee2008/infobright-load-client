package com.taobao.iblc;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DButil {
	protected final static Logger logger = LoggerFactory.getLogger(DButil.class);
	//private static Connection conn = null;
	//private static com.mysql.jdbc.PreparedStatement mysqlStatement = null;
	
	
	
    public static Connection getConnection(String dbIP,String dbport,String dbname,
    		String dbusr,String dbpwd) throws ClassNotFoundException, SQLException {  
		Connection con = null; 
		Class.forName("com.mysql.jdbc.Driver");
		// timeout unit ms
		String connStr = "jdbc:mysql://"
				+ dbIP
				+ ":"
				+ dbport
				+ "/"
				+ dbname
				+ "?connectTimeout=6000000&socketTimeout=6000000&autoReconnect=true";

		con = DriverManager.getConnection(connStr, dbusr, dbpwd);

		return con;    
	}  
    
    public static com.mysql.jdbc.PreparedStatement getLoadMysqlStatement(Connection conn,String loadDataSql){
    	PreparedStatement statement = null;
    	//logger.info(loadDataSql);  
    	try {
			statement = conn.prepareStatement(loadDataSql);
		} catch (SQLException e) {
			logger.info("创建PreparedStatement失败" + e.getMessage());
		}
    	com.mysql.jdbc.PreparedStatement mysqlStatement = null;

		try {
			if (statement.isWrapperFor(com.mysql.jdbc.Statement.class)) {

				mysqlStatement = statement
						.unwrap(com.mysql.jdbc.PreparedStatement.class);

			}
		} catch (SQLException e) {
			logger.info("unwrap com.mysql.jdbc.PreparedStatement 失败",e);  
		}
		return mysqlStatement;
    }
    
    public static int loadData(com.mysql.jdbc.PreparedStatement mysqlStatement,
    		                   InputStream is) throws SQLException{

    	if(mysqlStatement == null)
    		throw new RuntimeException("MysqlStatement null!!!");//error 

    	int result = 0;
		mysqlStatement.setLocalInfileInputStream(is);
		long begintime=System.currentTimeMillis();
        //logger.info("load start !!!");
		result = mysqlStatement.executeUpdate();
		//logger.info("load end !!!");
		long endtime=System.currentTimeMillis();
		double costTimeSec = (endtime - begintime)/1000.00;
		double speed = result/costTimeSec;

		logger.info("loaded data row size:"+result +" costTime "+costTimeSec+" seconds "+ speed+"rows/s");
    	return result;
    }
    /**
     * execute insert update delete or DDL
     * @param conn
     * @param sql
     * @return 
     * @throws SQLException 
     */
    public static int executeUpdate(Connection conn,String sql) throws SQLException{
    	int result = 0;
    	logger.info(sql); 
		java.sql.PreparedStatement pstat = null;
		try {
			pstat = conn.prepareStatement(sql);
			result = pstat.executeUpdate();
		} catch (SQLException e) {
			throw new SQLException(e); 
		}finally{
			try {
				pstat.close();
			} catch (SQLException e) {
				logger.info("PreparedStatement close failed!!!",e);
			}
		}
    	return result;
    }
    
    public static boolean existTable(Connection conn,String tblName) throws SQLException{
    	boolean flag;
    	String[] tmp = tblName.split("\\.");
    	String db = tmp[0];
    	String tbl = tmp[1];
    	//System.out.println("DB:"+db+",Table:"+tbl);
		DatabaseMetaData meta = conn.getMetaData();
		ResultSet rs = meta.getTables(db, null, tbl, new String[] { "TABLE" });//
		if(rs.next()){
			flag = true;
		}else{
			flag = false;
		}
		rs.close();
		return flag;
    }
    
    public static void main(String args[]) {
//    	long time = 7658;
//    	double cost = time/1000;
//    	double cost1 = time/1000.00;
//    	System.out.printf("%f\n %f\n", cost,cost1);
    	IBLConfig.init("loaddata.properties");
    	try {
			Connection conn = DButil.getConnection(IBLConfig.dbIP, IBLConfig.dbport, 
				    IBLConfig.dbname, IBLConfig.dbusername, IBLConfig.dbpassword);
			System.out.println(DButil.existTable(conn, "notify_trace_group_0-notify_msg_0_20130926"));
			System.out.println(DButil.existTable(conn, "notify_trace_group_0-notify_msg_0_20130930"));
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
}
