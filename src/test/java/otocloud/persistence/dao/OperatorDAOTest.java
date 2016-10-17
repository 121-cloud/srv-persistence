package otocloud.persistence.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/** 
* OtoCloudOperatorDAO Tester. 
* 
* @author zhangye 
* @since <pre>十月 28, 2015</pre> 
* @version 1.0 
*/
@RunWith(VertxUnitRunner.class)
public class OperatorDAOTest extends DAOTestBase {

    private OperatorDAO operatorDAO;

    @Before
    public void before(TestContext context) throws Exception {
        final Async async = context.async();
        super.setUp(context);
        operatorDAO = new OperatorDAO();
        operatorDAO.setDataSource(dataSource);
        async.complete();
    } 

   /** 
    * 
    * Method: insertBy(String tableName, JsonObject insertValues, int operatorId, Future<UpdateResult> doneFuture) 
    * 
    */ 
    @Test
    public void testInsertBy(TestContext context) throws Exception {
        final Async async = context.async();
        Future<UpdateResult> doneFuture = Future.future();
        operatorDAO.insertBy("auth_user", new JsonObject().put("name", "name by operator dao"), 0,
                doneFuture);
        doneFuture.setHandler(ret -> {
            if(ret.succeeded()){
                int num = ret.result().getUpdated();
                context.assertNotEquals(0, num);
                async.complete();
            }else{
                context.fail("无法添加新数据。");
            }
        });
    }

    /**
    * 
    * Method: updateBy(String tableName, JsonObject setValues, JsonObject where, int operatorId, Future<UpdateResult> doneFuture) 
    * 
    */ 
    @Test
    public void testUpdateBy(TestContext context) throws Exception {
        final Async async = context.async();
        Future<UpdateResult> doneFuture = Future.future();
        JsonObject setValues = new JsonObject();
        setValues.put("password", "*234");
        JsonObject where = new JsonObject();
        where.put("name", "name by operator dao");
        operatorDAO.updateBy("auth_user", setValues, where, 0, doneFuture);
        doneFuture.setHandler(ret -> {
            if(ret.succeeded()){
                int num = ret.result().getUpdated();
                context.assertNotEquals(0, num);
                async.complete();
            }else{
                context.fail("无法更新数据。");
            }
        });
    }

    /**
    * 
    * Method: deleteBy(String tableName, JsonObject where, int operatorId, Future<UpdateResult> doneFuture) 
    * 
    */ 
    @Test
    public void testDeleteBy(TestContext context) throws Exception {
        final Async async = context.async();
        Future<UpdateResult> doneFuture = Future.future();
        JsonObject where = new JsonObject();
        where.put("name", "name by operator dao");
        operatorDAO.deleteBy("auth_user", where, 0, doneFuture);
        doneFuture.setHandler(ret -> {
            if(ret.succeeded()){
                int num = ret.result().getUpdated();
                context.assertNotEquals(0, num);
                async.complete();
            }else{
                context.fail("无法删除数据。");
            }
        });
    }


}
