/*package otocloud.persistence.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

*//**
 * Created by zhangye on 2015-10-26.
 *//*
@RunWith(VertxUnitRunner.class)
public class BaseDAOTest extends DAOTestBase {

    protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private BaseDAO baseDAO;

    @Before
    public void setUp(TestContext context) {
        final Async async = context.async();
        super.setUp(context);
        baseDAO = new BaseDAO();
        baseDAO.setDataSource(dataSource);

        async.complete();
    }

    @Test
    public void it_should_connect_to_mysql(TestContext context) {
        final Async async = context.async();

        JsonObject connConfig = new JsonObject();
        connConfig.put("url", "jdbc:mysql://10.10.23.112:3306/121db_new?useUnicode=true&characterEncoding=UTF-8");
        connConfig.put("driver_class", "com.mysql.jdbc.Driver");
        connConfig.put("max_pool_size", 30);
        connConfig.put("user", "root");
        connConfig.put("password", "root");

        JDBCClient client = JDBCClient.createShared(vertx, connConfig, "MyDataSource");

        client.getConnection(res -> {
            if (res.succeeded()) {

               async.complete();

            } else {
                // Failed to get connection - deal with it
                context.fail();
            }
        });
    }

    @Test
    public void it_should_add_an_user(TestContext context) {
        final Async async = context.async();

        Future<UpdateResult> doneFuture = Future.future();

        baseDAO.insertBy("auth_user", new JsonObject().put("name", "name by baseDAO"), doneFuture);
        doneFuture.setHandler(ret -> {
            if (ret.succeeded()) {
                async.complete();
            } else {
                context.fail();
            }
        });
    }

    @Test
    public void it_should_update_auth_user_data_partially(TestContext context) throws Exception {
        final Async async = context.async();

        Future<UpdateResult> doneFuture = Future.future();
        baseDAO.updateBy("auth_user", new JsonObject()
                        .put("email", "updateByBaseDAO@yonyou.com")
                        .put("last_pwd_changed_datetime", "2015-10-26 11:03"),
                new JsonObject().put("id", "1"), doneFuture);

        doneFuture.setHandler(ret -> {
            if (ret.succeeded()) {
                logger.info(ret.result().toJson());
                async.complete();
            } else {
                context.fail(ret.cause());
            }
        });
    }
}
*/