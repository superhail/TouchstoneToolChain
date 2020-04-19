package ecnu.db.query.analyzer.online;

import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.query.analyzer.statical.QueryAliasParser;
import ecnu.db.schema.Schema;
import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractAnalyzer {
    protected AbstractDbConnector dbConnector;
    protected Map<String, String> aliasDic;
    protected QueryAliasParser queryAliasParser = new QueryAliasParser();
    protected HashMap<String, Schema> schemas;
    protected int sqlArgIndex = 0;
    protected int lastArgIndex = 0;
    protected HashMap<String, List<String>> argsAndIndex = new HashMap<>();


    AbstractAnalyzer(AbstractDbConnector dbConnector, HashMap<String, Schema> schemas) {
        this.dbConnector = dbConnector;
        this.schemas = schemas;
    }

    abstract String[] getSqlInfoColumns();

    public abstract String getDbType();

    /**
     * 查询树的解析
     *
     * @param queryPlan query解析出的查询计划，带具体的行数
     * @return 查询树Node信息
     * @throws TouchstoneToolChainException 查询树无法解析
     */
    public abstract ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneToolChainException;

    public List<String[]> getQueryPlan(String sql) throws SQLException {
        aliasDic = queryAliasParser.getTableAlias(sql, getDbType());
        return dbConnector.explainQuery(sql, getSqlInfoColumns());
    }

    public HashMap<String, List<String>> getArgsAndIndex() {
        return argsAndIndex;
    }

    public abstract List<String> outputNode(ExecutionNode root) throws TouchstoneToolChainException, SQLException;

    public void outputSuccess(boolean success) {
        if (success) {
            lastArgIndex = sqlArgIndex;
        } else {
            sqlArgIndex = lastArgIndex;
        }
        for (Schema schema : schemas.values()) {
            schema.keepJoinTag(success);
        }
    }
}
