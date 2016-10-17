/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.persistence.dao;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import otocloud.common.OtoConfiguration;

/**
 * TODO: DOCUMENT ME!
 * @date 2015年6月21日
 * @author lijing@yonyou.com
 */
public class MongoDataSource {
	//mongo客户端
	protected Vertx vertx;
	protected MongoClient mongoClient; 
	protected String mongoSharePool;
	protected JsonObject mongoCfg;

    protected void init(Vertx vertx, JsonObject datasourceCfg) {
		this.vertx = vertx;
        this.mongoSharePool = datasourceCfg.getString(OtoConfiguration.SHAREDPOOL);    
        this.mongoCfg = datasourceCfg.getJsonObject(OtoConfiguration.CONFIG); 
    	createMongoClient();
	}
	
    synchronized public void close() {	
		closeMongoClient();		
	}
	
	public MongoClient getMongoClient() {
		if(mongoClient != null)
			return mongoClient;		
		createMongoClient();
		return mongoClient;
	}
	
	public MongoClient newMongoClient() {
		return MongoClient.createShared(vertx, mongoCfg, mongoSharePool);
	}
	
    //创建MongoClient
	synchronized private void createMongoClient() {    
		if(mongoClient == null)
			mongoClient = MongoClient.createShared(vertx, mongoCfg, mongoSharePool);
	}    
    
    private void closeMongoClient() {
       	if(mongoClient != null){
    		mongoClient.close();
    		mongoClient = null;
    	}
	}    

}
