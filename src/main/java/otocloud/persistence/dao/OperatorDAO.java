package otocloud.persistence.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.UpdateResult;

/**
 * 在操作数据库时，自动处理数据库表的后续六个字段。
 * 这六个字段是 entry_id, entry_datetime, update_id, update_datetime, delete_id, delete_datetime。
 * 其中 *_id 表示操作人ID, *_datetime 表示操作时间。
 * <p/>
 * 当插入数据时，自动添加 entry_id, entry_datetime 字段。
 * 当更新数据时，自动添加 update_id, update_datetime 字段。
 * 当删除数据时，自动添加 delete_id, delete_datetime 字段。
 * <p/>
 * zhangyef@yonyou.com on 2015-10-28.
 */
public class OperatorDAO extends BaseDAO {

    public OperatorDAO() {
    }

    public OperatorDAO(JdbcDataSource dataSource) {
        super(dataSource);
    }

    protected final void insertBy(String tableName, JsonObject insertValues, int operatorId,
                                  Future<UpdateResult> doneFuture) {

        JsonObject insertValuesWithID = insertValues;
        insertValuesWithID.put("entry_id", operatorId);

        insertBy(tableName, insertValuesWithID, doneFuture);
    }

    protected final void insertBy(TransactionConnection transConn,
                                  String tableName, JsonObject insertValues, int operatorId,
                                  Future<UpdateResult> doneFuture) {
        JsonObject insertValuesWithID = insertValues;
        insertValuesWithID.put("entry_id", operatorId);

        insertBy(transConn, tableName, insertValuesWithID, doneFuture);
    }

    protected final void updateBy(String tableName, JsonObject setValues, JsonObject where,
                                  int operatorId,
                                  Future<UpdateResult> doneFuture) {
        JsonObject setValuesWithId = setValues;
        setValuesWithId.put("update_id", operatorId);

        updateBy(tableName, setValuesWithId, where, doneFuture);
    }

    /**
     * 在指定事务中更新数据库，自动更新update_id字段。
     *
     * @param transConn
     * @param tableName
     * @param setValues
     * @param where
     * @param operatorId
     * @param doneFuture
     */
    protected final void updateBy(TransactionConnection transConn,
                                  String tableName, JsonObject setValues,
                                  JsonObject where, int operatorId,
                                  Future<UpdateResult> doneFuture) {
        JsonObject setValuesWithId = setValues;
        setValuesWithId.put("update_id", operatorId);

        updateBy(transConn, tableName, setValuesWithId, where, doneFuture);
    }

    protected final void deleteBy(String tableName, JsonObject where, int operatorId,
                                  Future<UpdateResult> doneFuture) {
        String deleteSQL = makeDeleteSQL(tableName, where, operatorId);
        JsonArray params = makeWhereValues(where);
        updateWithParams(deleteSQL, params, doneFuture);
    }

    protected final void deleteBy(TransactionConnection transConn,
                                  String tableName, JsonObject where, int operatorId,
                                  Future<UpdateResult> doneFuture) {
        String deleteSQL = makeDeleteSQL(tableName, where, operatorId);
        JsonArray params = makeWhereValues(where);

        updateWithParams(transConn, deleteSQL, params, doneFuture);
    }

    private String makeDeleteSQL(String tableName, JsonObject where, int operatorId) {
        return "UPDATE " + tableName
                + " SET " + "delete_id=" + operatorId + ", delete_datetime=NOW() "
                + " WHERE " + makeWhereConditionClause(where);
    }


}
