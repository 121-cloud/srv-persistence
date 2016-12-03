package otocloud.persistence.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.MongoClientImpl;

/**
 * 对vert.x的原生MongoClient的封装
 * @author pcitc
 *
 */
public class OTOMongoClientImpl extends MongoClientImpl {

	public OTOMongoClientImpl(Vertx vertx, JsonObject config, String dataSourceName) {
		super(vertx, config, dataSourceName);
	}
	
	/**
	 * 支持批量保存
	 */
	public io.vertx.ext.mongo.MongoClient save(String collection, JsonArray documents, Handler<AsyncResult<String>> resultHandler) {
		for (Object document : documents) {
			super.save(collection, (JsonObject)document, resultHandler);
		}
		return this;
	}

}
