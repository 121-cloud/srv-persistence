package otocloud.persistence.dao;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;

import org.apache.commons.lang3.StringUtils;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * Created by zhangye on 2015-10-15.
 */

public class BaseDAO {
    protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

    private JdbcDataSource dataSource;

    public BaseDAO() {
    }

    public BaseDAO(JdbcDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public JdbcDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(JdbcDataSource dataSource) {
        this.dataSource = dataSource;
    }    
    
    protected final void deleteWithParams(String sql, JsonArray params, Future<UpdateResult> done) {
        updateWithParams(sql, params, done);
    }

    /**
     * TODO 添加测试
     * <p/>
     * 构造 UPDATE 的 SET 子句.
     *
     * @param values 将要更新的数据.
     * @return "key1=?, key2=?, update_datetime=NOW()"
     */
    private String makeUpdateSetClause(JsonObject values) {
        StringBuilder builder = new StringBuilder();

        builder.append("id=id");

        values.forEach(entry -> {
            String key = entry.getKey();
            builder.append(", ").append(key).append("=").append("?"); //", key=?"
        });

        builder.append(", update_datetime=NOW()");

        return builder.toString();
    }

    /**
     * 生成 UPDATE 中 SET 子句中的值.
     *
     * @param set
     * @return
     */
    private JsonArray makeUpdateSetValues(JsonObject set) {
        return makeValuesFromJson(set);
    }

    /**
     * 将JsonObject对象的value按照原始顺序全部提取，生成JsonArray.
     *
     * @param values
     * @return
     */
    private JsonArray makeValuesFromJson(JsonObject values) {
        List<Object> paramList = new LinkedList<>();
        values.forEach(entry -> paramList.add(entry.getValue()));

        return new JsonArray(paramList);
    }


    /**
     * 取出where子句的参数值。注意，值的排列顺序需要与参数的顺序保持一致.
     * <p/>
     * 如果值为null, 则不添加. 作为查询条件的null已经在makeWhereConditionClause设置.
     *
     * @param where
     * @return
     * @see BaseDAO#makeWhereConditionClause(JsonObject)
     */
    protected final JsonArray makeWhereValues(JsonObject where) {
        List<Object> paramList = new LinkedList<>();
        where.forEach(entry -> {
            Object value = entry.getValue();
            if (value != null) {
                paramList.add(entry.getValue());
            }
        });

        return new JsonArray(paramList);
    }

    /**
     * 如果参数为null或者大小为空，则查询所有的列。
     * 返回的结果前后都没有空格.
     *
     * @param columns
     * @return
     */
    protected final String makeSelectColumnClause(String[] columns) {
        StringBuilder builder = new StringBuilder();
        if (columns == null || columns.length == 0) {
            builder.append("*");
        } else {
            int columnNum = columns.length;
            for (int i = 0; i < columnNum; i++) {
                builder.append(columns[i]).append(i + 1 != columnNum ? ", " : "");
            }
        }
        return builder.toString().trim();
    }

    /**
     * @param where
     * @return 输出的格式为“para1=? AND para2=? AND para3 is NULL”
     */
    protected final String makeWhereConditionClause(JsonObject where) {
        if (where == null || where.isEmpty()) {
            return "1=1";
        }

        StringBuilder builder = new StringBuilder();
        Set<String> fields = where.fieldNames();
        String[] fieldNames = fields.toArray(new String[fields.size()]);
        int fieldNum = fieldNames.length;
        for (int i = 0; i < fieldNum; i++) {
            //判断where的查询字段是否为null, 判断为null的语法不是"=", 在MySQL中是"is NULL"
            Object value = where.getValue(fieldNames[i]);
            if (value == null) {
                builder.append(fieldNames[i] + " is NULL");
            } else {
                builder.append(fieldNames[i] + "=?");
            }

            //如果不是最后一个元素, 添加AND关键字.
            builder.append(i + 1 != fieldNum ? " AND " : "");
        }

        return builder.toString();
    }

    /**
     * 在指定数据库表中查找满足条件的记录.
     *
     * @param tableName      数据库表名.
     * @param columns        如果为null或者空数组, 表示查询所有列.
     * @param where          以(columnName, columnValue)方式存储的查询约束条件.
     * @param completeFuture
     * @see #queryBy(String, String[], JsonObject, String, Future)
     */
    protected final void queryBy(String tableName, final String[] columns, final JsonObject where,
                                 Future<ResultSet> completeFuture) {
        queryBy(tableName, columns, where, null, completeFuture);
    }

    /**
     * 在指定数据库表中查找满足条件的记录.
     * 可以在 otherWhere 中拼写带有关键字 "in" 的查询子句.
     *
     * @param tableName      数据库表名.
     * @param columns        如果为null或者空数组, 表示查询所有列.
     * @param where          以(columnName, columnValue)方式存储的查询约束条件.
     * @param otherWhere     由调用方提供的附加的查询条件. 需要以"AND" 为字符串前缀.
     * @param completeFuture 返回查询结果, ResultSet.
     */
    protected final void queryBy(String tableName, final String[] columns,
                                 final JsonObject where, final String otherWhere,
                                 Future<ResultSet> completeFuture) {
        StringBuilder querySQLBuilder = new StringBuilder();

        querySQLBuilder.append("SELECT " + makeSelectColumnClause(columns));
        querySQLBuilder.append(" FROM " + tableName);
        querySQLBuilder.append(" WHERE " + makeWhereConditionClause(where));

        if (StringUtils.isNotBlank(otherWhere)) {
            querySQLBuilder.append(" " + otherWhere);
        }

        JsonArray params = makeWhereValues(where);

        queryWithParams(querySQLBuilder.toString(), params, completeFuture);
    }

    /**
     * 不使用事务进行查询.
     *
     * @param sql            SQL语句.
     * @param params         SQL语句中?对应的参数.
     * @param completeFuture 返回查询结果.
     */
    protected final void queryWithParams(String sql, JsonArray params, Future<ResultSet> completeFuture) {
        createDBConnect(conn -> {
                    conn.queryWithParams(sql, params, ret -> {
                        if (ret.succeeded()) {
                            completeFuture.complete(ret.result());
                        } else {
                            completeFuture.fail(new RuntimeException(ret.cause()));
                        }

                        closeDBConnect(conn);
                    });
                }

        );
    }

    /**
     * 在指定的事务连接中执行插入操作.
     * 事务连接需要在调用方关闭.
     *
     * @param transConn    事务连接.
     * @param tableName    插入数据的表名.
     * @param insertValues 即将插入的数据.
     * @param doneFuture   插入完成的回调函数.
     */
    protected final void insertBy(TransactionConnection transConn,
                                  String tableName, JsonObject insertValues,
                                  Future<UpdateResult> doneFuture) {
        //INSERT INTO table_name (列1, 列2,...) VALUES (值1, 值2,....)
        String insertSQL = "INSERT INTO " + tableName + " " + makeInsertClause(insertValues);
        JsonArray params = makeInsertParams(insertValues);

        updateWithParams(transConn, insertSQL, params, doneFuture);
    }


    protected final void insertBy(String tableName, JsonObject insertValues, Future<UpdateResult> doneFuture) {
        String insertSQL = "INSERT INTO " + tableName + " " + makeInsertClause(insertValues);
        JsonArray params = makeInsertParams(insertValues);
        updateWithParams(insertSQL, params, doneFuture);
    }

    private JsonArray makeInsertParams(JsonObject insertValues) {
        return makeValuesFromJson(insertValues);
    }

    /**
     * @param insertValues
     * @return "(列1, 列2,..., entry_datetime) VALUES (?, ?,..., now())"
     */
    private String makeInsertClause(JsonObject insertValues) {
        Set<String> fields = insertValues.fieldNames();

        if (fields.size() < 1) {
            return "";
        }

        String[] fieldArray = fields.toArray(new String[fields.size()]);

        //构造"(列1, 列2, ...)"
        StringBuilder builder = new StringBuilder();
        builder.append("(").append(fieldArray[0]);

        for (int i = 1; i < fieldArray.length; i++) {
            builder.append(",").append(fieldArray[i]);
        }

        builder.append(", entry_datetime)");

        //构造"(?,?,...)"
        StringBuilder valueBuilder = new StringBuilder();
        valueBuilder.append("(?");

        for (int i = 1; i < fieldArray.length; i++) {
            valueBuilder.append(",?");
        }
        valueBuilder.append(",NOW())");//使用数据库时间。

        //构造"(列1, 列2, ...) VALUES (?,?,...)"
        builder.append(" VALUES ").append(valueBuilder.toString());

        return builder.toString();
    }

    /**
     * 更新指定表的数据。
     * 自动添加对于update_datetime字段的更新。
     *
     * @param tableName  数据库表的名称.
     * @param setValues  需要更新的值.
     * @param where      更新子句的约束条件.
     * @param doneFuture 更新结束后调用.
     */
    protected final void updateBy(String tableName, JsonObject setValues, JsonObject where, Future<UpdateResult>
            doneFuture) {
        String updateSQL = "UPDATE " + tableName
                + " SET " + makeUpdateSetClause(setValues)
                + " WHERE " + makeWhereConditionClause(where);

        JsonArray setParams = makeUpdateSetValues(setValues);
        JsonArray whereParams = makeWhereValues(where);

        JsonArray params = new JsonArray().addAll(setParams).addAll(whereParams);

        updateWithParams(updateSQL, params, doneFuture);
    }

    /**
     * 在指定的事务连接中执行更新操作.
     *
     * @param transConn  由调用方创建的事务连接.
     * @param tableName  数据库表的名称.
     * @param setValues  UPDATE 子句将要设置的值.
     * @param where      更新约束条件.
     * @param doneFuture 操作结束后的回调.
     */
    protected final void updateBy(TransactionConnection transConn,
                                  String tableName, JsonObject setValues,
                                  JsonObject where,
                                  Future<UpdateResult> doneFuture) {
        String updateSQL = "UPDATE " + tableName
                + " SET " + makeUpdateSetClause(setValues)
                + " WHERE " + makeWhereConditionClause(where);

        JsonArray setParams = makeUpdateSetValues(setValues);
        JsonArray whereParams = makeWhereValues(where);

        JsonArray params = new JsonArray().addAll(setParams).addAll(whereParams);

        updateWithParams(transConn, updateSQL, params, doneFuture);

    }

    /**
     * 在指定的事务连接中执行更新操作。该事务可能由一个或多个DAO共同完成.
     * 需要由调用方传入事务连接{@link TransactionConnection}，并在外部关闭事务.
     * <p/>
     * 支持 INSERT/UPDATE/DELETE 操作.
     *
     * @param transConn 由调用方创建的事务连接.
     * @param sql       SQL语句.
     * @param params    SQL语句中的参数.
     * @param done      操作结束后的回调.
     */
    protected final void updateWithParams(TransactionConnection transConn, String sql, JsonArray params,
                                          Future<UpdateResult> done) {
        transConn.getConn().updateWithParams(sql, params, ret -> {
            if (ret.succeeded()) {
                done.complete(ret.result());
            } else {
                done.fail(new RuntimeException(ret.cause()));
            }
        });
    }

    /**
     * 如果sql语句中没有问号表示的参数，则params可以为null。
     *
     * @param sql
     * @param params
     * @param done
     */
    protected final void updateWithParams(String sql, JsonArray params, Future<UpdateResult> done) {
        createDBConnect(conn -> conn.setAutoCommit(true, res -> {
            if (res.failed()) {
                done.fail(res.cause());
                return;
            }

            conn.updateWithParams(sql, params, ret -> {
                if (ret.succeeded()) {
                    try {
                        done.complete(ret.result());
                    } finally {
                        //关闭连接
                        closeDBConnect(conn);
                    }
                } else {
                    done.fail(ret.cause());
                }
            });

        }), e -> logger.error("连接数据库错误.", e));
    }

    protected final void createDBConnect(Handler<SQLConnection> connectionHandler) {
        createDBConnect(connectionHandler, e -> {
        });
    }

    protected final void createDBConnect(Handler<SQLConnection> connectedHandler, Handler<Throwable> errHandler) {
        JDBCClient jdbcClient = dataSource.getSqlClient();
        jdbcClient.getConnection(conRes -> {
            if (conRes.succeeded()) {
                final SQLConnection connection = conRes.result();
                connectedHandler.handle(connection);
            } else {
                Throwable err = conRes.cause();
                String errMsg = err.getMessage();
                logger.error(errMsg, err);
                if (errHandler != null) {
                    errHandler.handle(err);
                }
            }
        });
    }

    final protected void closeDBConnect(SQLConnection conn) {
        conn.setAutoCommit(true, ret -> {
            conn.close(handler -> {
                if (handler.failed()) {
                    Throwable conErr = handler.cause();
                    logger.error(conErr.getMessage(), conErr);
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("连接关闭成功.");
                    }
                }
            });
        });
    }


}
