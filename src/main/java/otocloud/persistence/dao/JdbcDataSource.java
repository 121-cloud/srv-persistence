/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.persistence.dao;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;

/**
 * TODO: DOCUMENT ME!
 * @date 2015年6月21日
 * @author lijing@yonyou.com
 */
public class JdbcDataSource {
	private JDBCClient sqlClient;
	
	public static JdbcDataSource createDataSource(Vertx vertx, JsonObject datasourceCfg){
		JdbcDataSource retDs = new JdbcDataSource();
		retDs.init(vertx, datasourceCfg);
		return retDs;
	}
	
    public void init(Vertx vertx, JsonObject datasourceCfg) {
    	configSqlClient(vertx, datasourceCfg);
	}
	
    public void close() {	
    	closeSqlClient();		
	}
	
	private void closeSqlClient(){
		sqlClient.close();	
	}	
	
	public JDBCClient getSqlClient(){
		return sqlClient;
	}	

	private void configSqlClient(Vertx vertx, JsonObject dsCfg) {
		JsonObject mysqlCfg = dsCfg.getJsonObject("config");
		String mysqlSharePool = dsCfg.getString("sharedpool");
		sqlClient = JDBCClient.createShared(vertx, mysqlCfg, mysqlSharePool);	
	}

}
