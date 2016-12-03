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
 * 
 * @date 2015年6月21日
 * @author lijing@yonyou.com
 */
public class MongoDataSource {
	// mongo客户端
	protected Vertx vertx;
	protected MongoClient mongoClient;
	protected OTOMongoClientImpl mongoClient_oto;// oto封装的client
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
	
	synchronized public void close_oto() {
		closeMongoClient_oto();
	}

	public MongoClient getMongoClient() {
		if (mongoClient != null)
			return mongoClient;
		createMongoClient();
		return mongoClient;
	}

	public OTOMongoClientImpl getMongoClient_oto() {
		if (mongoClient_oto != null)
			return mongoClient_oto;
		createMongoClient_oto();
		return mongoClient_oto;
	}

	public MongoClient newMongoClient() {
		return MongoClient.createShared(vertx, mongoCfg, mongoSharePool);
	}

	// 创建MongoClient
	synchronized private void createMongoClient() {
		if (mongoClient == null)
			mongoClient = MongoClient.createShared(vertx, mongoCfg, mongoSharePool);
	}

	// 创建MongoClient_oto
	synchronized private void createMongoClient_oto() {
		if (mongoClient_oto == null)
			mongoClient_oto = new OTOMongoClientImpl(vertx, mongoCfg, mongoSharePool);
	}

	private void closeMongoClient() {
		if (mongoClient != null) {
			mongoClient.close();
			mongoClient = null;
		}
	}
	
	private void closeMongoClient_oto() {
		if (mongoClient_oto != null) {
			mongoClient_oto.close();
			mongoClient_oto = null;
		}
	}

}
