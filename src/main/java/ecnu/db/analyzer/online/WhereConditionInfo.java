package ecnu.db.analyzer.online;

/**
 * @author xuechao.lian
 * 记录关于where_condition的信息的struct
 */
class WhereConditionInfo {
    public boolean isOr;
    public boolean useAlias;
    public String tableName;
}
