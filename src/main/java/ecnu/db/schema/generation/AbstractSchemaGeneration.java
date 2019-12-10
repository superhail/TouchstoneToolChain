package ecnu.db.schema.generation;

import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.*;
import ecnu.db.utils.ConfigConvert;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author wangqingshuai
 */
public abstract class AbstractSchemaGeneration {
    /**
     * format sql and return two sqls
     *
     * @param createTableSql create table sql
     * @return 1.column info sqls 2. keys info sql, including primary key and foreign keys
     */
    abstract Pair<String[], String> getColumnSqlAndKeySql(String createTableSql);

    /**
     * @param columnSqls column info sqls
     * @return a collection of column names and column types, all the types must exist in the utils
     */
    abstract HashMap<String, String> getColumnInfo(String[] columnSqls);

    /**
     * @param keysInfoSql keys sql
     * @return all the primary keys and foreign keys info
     * If there is not foreign keys info in the sql, the arg is null.
     */
    abstract Pair<ArrayList<String>, HashMap<String, String>> getPrimaryKeyAndForeignKey(String keysInfoSql);

    public Schema generateSchema(String tableName, String sql) throws Exception {
        Pair<String[], String> columnSqlAndKeySql = getColumnSqlAndKeySql(sql);
        Pair<ArrayList<String>, HashMap<String, String>> primaryKeyAndForeignKey =
                getPrimaryKeyAndForeignKey(columnSqlAndKeySql.getRight());
        ArrayList<AbstractColumn> columns = getColumns(getColumnInfo(columnSqlAndKeySql.getLeft()));
        return new Schema(tableName, primaryKeyAndForeignKey.getLeft(), primaryKeyAndForeignKey.getRight(), columns);
    }

    private ArrayList<AbstractColumn> getColumns(HashMap<String, String> columnNameAndTypes) throws Exception {
        ArrayList<AbstractColumn> columns = new ArrayList<>();
        for (Map.Entry<String, String> columnNameAndType : columnNameAndTypes.entrySet()) {
            switch (ConfigConvert.getColumnType(columnNameAndType.getValue())) {
                case Int:
                    columns.add(new IntColumn(columnNameAndType.getKey()));
                    break;
                case Bool:
                    columns.add(new BoolColumn(columnNameAndType.getKey()));
                    break;
                case Decimal:
                    columns.add(new DecimalColumn(columnNameAndType.getKey()));
                    break;
                case String:
                    columns.add(new StringColumn(columnNameAndType.getKey()));
                    break;
                case Date:
                    columns.add(new DateColumn(columnNameAndType.getKey()));
                    break;
                default:
                    throw new Exception("没有实现的类型转换");
            }
        }
        return columns;
    }


    public abstract String getColumnDistributionSql(ArrayList<AbstractColumn> columns) throws Exception;

    public abstract void setDataRangeBySqlResult(ArrayList<AbstractColumn> columns, String[] sqlResult) throws Exception;

    public abstract void setDataRangeUnique(Schema schema, AbstractDbConnector dbConnector) throws IOException, SQLException;
}
