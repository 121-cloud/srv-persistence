package otocloud.persistence.dao;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.CompositeFutureImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.impl.MongoClientImpl;

/**
 * 对vert.x的原生MongoClient的封装
 * 
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
	public void save(String collection, JsonArray documents,
			Handler<AsyncResult<JsonArray>> resultHandler) {
		Future<JsonArray> future = Future.future();
		future.setHandler(resultHandler);
		List<Future> futures = new ArrayList<>();
		for (Object document : documents) {
			Future<JsonObject> repRelationFuture = Future.future();
			futures.add(repRelationFuture);
			super.save(collection, (JsonObject) document, result -> {
				if (result.succeeded()) {
					((JsonObject) document).put("_id", result.result());
					repRelationFuture.complete((JsonObject) document);
				} else {
					repRelationFuture.fail(result.cause());
				}
			});
		}
		JsonArray ret = new JsonArray();
		CompositeFuture.join(futures).setHandler(ar -> {
			CompositeFutureImpl comFutures = (CompositeFutureImpl) ar;
			if (comFutures.size() > 0) {
				for (int i = 0; i < comFutures.size(); i++) {
					if (comFutures.succeeded(i)) {
						JsonObject document = comFutures.result(i);
						ret.add(document);
					}
				}
			}
			future.complete(documents);
		});
	}

}
