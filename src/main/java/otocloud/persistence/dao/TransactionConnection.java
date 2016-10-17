/*
 * Copyright (C) 2015 121Cloud Project Group  All rights reserved.
 */
package otocloud.persistence.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.SQLConnection;

/**
 * TODO: DOCUMENT ME!
 * @date 2015年6月21日
 * @author lijing@yonyou.com
 */
public class TransactionConnection {
	private SQLConnection conn;
	
	public SQLConnection getConn() {
		return conn;
	}

	public static void createTransactionConnection(SQLConnection conn, Handler<AsyncResult<TransactionConnection>> transConn){
		Future<TransactionConnection> retFuture = Future.future();
		retFuture.setHandler(transConn);
		
		conn.setAutoCommit(false, autoCmt->{
			if(autoCmt.succeeded()){
				TransactionConnection connWrapper = new TransactionConnection(conn);
				retFuture.complete(connWrapper);
			}else{
				retFuture.fail(autoCmt.cause());
			}			
		});		
	}
	
	public TransactionConnection(SQLConnection conn){
		this.conn = conn;
	}
	
    public void commit(Handler<AsyncResult<Void>> commitRet) {	
		conn.commit(cmtRet -> {
			commitRet.handle(cmtRet);
		});	 
	}
    
    public void commitAndClose(Handler<AsyncResult<Void>> commitRet) {	
 		conn.commit(cmtRet -> {
 			if(cmtRet.failed()){
 				cmtRet.cause().printStackTrace();
 			}
			close(handler->{	
				commitRet.handle(handler);
			});
 		});	 
 	}    

    public void rollback(Handler<AsyncResult<Void>> commitRet) {	
		conn.rollback(cmtRet -> {
			commitRet.handle(cmtRet);
		});	 
	}

    public void rollbackAndClose(Handler<AsyncResult<Void>> commitRet) {	
 		conn.rollback(cmtRet -> {
 			if(cmtRet.failed()){
 				cmtRet.cause().printStackTrace();
 			}
			close(handler->{	
				commitRet.handle(handler);
			});
 		});	 
 	}
    
    public void close(Handler<AsyncResult<Void>> closedRet) {	
		Future<Void> retFuture = Future.future();
		retFuture.setHandler(closedRet);
		
		conn.setAutoCommit(true, ret->{
			if(ret.succeeded()){
				conn.close(handler->{				
					if (handler.failed()) {
						retFuture.fail(handler.cause());					
					} else {
						retFuture.complete();
					}
				});	
			}else{
				retFuture.fail(ret.cause());
			}
		});
		
	}
	


}
