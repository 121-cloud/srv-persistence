package otocloud.persistence.dao;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import org.junit.After;
import org.junit.Before;

/**
 * zhangyef@yonyou.com on 2015-10-28.
 */
public class DAOTestBase {
    protected JdbcDataSource dataSource;
    protected Vertx vertx;

    @Before
    public void setUp(TestContext context) {
        vertx = Vertx.vertx();
        dataSource = makeSysDBSource();
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }


    private JdbcDataSource makeSysDBSource() {
        JsonObject sysDSConfig = new JsonObject();
        sysDSConfig.put("sharedpool", "jdbc-auth");
        JsonObject connConfig = new JsonObject();
        connConfig.put("url", "jdbc:mysql://10.10.23.112:3306/121db_new?useUnicode=true&characterEncoding=UTF-8");
        connConfig.put("driver_class", "com.mysql.jdbc.Driver");
        connConfig.put("max_pool_size", 30);
        connConfig.put("user", "test");
        connConfig.put("password", "test");
        sysDSConfig.put("config", connConfig);

        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.init(vertx, sysDSConfig);
        return dataSource;
    }
}
