package ecnu.db.schema.generation;

import com.alibaba.fastjson.JSON;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.column.*;
import ecnu.db.utils.TidbStatsJsonObject;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author wangqingshuai
 */
public class TidbSchemaGeneration extends AbstractSchemaGeneration {
    @Override
    Pair<String[], String> getColumnSqlAndKeySql(String createTableSql) {
        createTableSql = createTableSql.toLowerCase();
        createTableSql = createTableSql.substring(createTableSql.indexOf("\n") + 1, createTableSql.lastIndexOf(")"));
        createTableSql = createTableSql.replaceAll("`", "");
        String[] sqls = createTableSql.split("\n");
        String keysInfo = sqls[sqls.length - 1].trim();
        return new MutablePair<>(Arrays.copyOfRange(sqls, 0, sqls.length - 1), keysInfo);
    }

    @Override
    HashMap<String, String> getColumnInfo(String[] columnSqls) {
        HashMap<String, String> columnInfos = new HashMap<>();
        for (String columnSql : columnSqls) {
            String[] attributes = columnSql.trim().split(" ");
            String columnName = attributes[0];
            int indexOfBrackets = attributes[1].indexOf('(');
            columnInfos.put(columnName, (indexOfBrackets > 0) ?
                    attributes[1].substring(0, indexOfBrackets) : attributes[1]);
        }
        return columnInfos;
    }

    /**
     * Because there is not foreign keys info in tidb, so the value is always null.
     *
     * @param keysInfoSql keys sql
     * @return primarykeys info
     */
    @Override
    Pair<ArrayList<String>, HashMap<String, String>> getPrimaryKeyAndForeignKey(String keysInfoSql) {
        keysInfoSql = keysInfoSql.substring(keysInfoSql.indexOf("(") + 1, keysInfoSql.indexOf(")"));
        ArrayList<String> keys = new ArrayList<>();
        Collections.addAll(keys, keysInfoSql.split(","));
        return new MutablePair<>(keys, null);
    }

    @Override
    public String getColumnDistributionSql(ArrayList<AbstractColumn> columns) throws Exception {
        StringBuilder sql = new StringBuilder();
        for (AbstractColumn column : columns) {
            switch (column.getColumnType()) {
                case Date:
                case Decimal:
                case Int:
                    sql.append("min(").append(column.getColumnName())
                            .append("),max(").append(column.getColumnName()).append("),");
                    break;
                case String:
                    sql.append("max(length(").append(column.getColumnName()).append(")),");
                    break;
                case Bool:
                    break;
                default:
                    throw new Exception("未匹配到的类型");
            }
        }
        return sql.toString().substring(0, sql.length() - 1);
    }

    @Override
    public void setDataRangeBySqlResult(ArrayList<AbstractColumn> columns, String[] sqlResult) throws Exception {
        int index = 0;
        for (AbstractColumn column : columns) {
            switch (column.getColumnType()) {
                case Int:
                    ((IntColumn) column).setMin(Integer.parseInt(sqlResult[index++]));
                    ((IntColumn) column).setMax(Integer.parseInt(sqlResult[index++]));
                    break;
                case String:
                    ((StringColumn) column).setMaxLength(Integer.parseInt(sqlResult[index++]));
                    break;
                case Decimal:
                    ((DecimalColumn) column).setMin(Double.parseDouble(sqlResult[index++]));
                    ((DecimalColumn) column).setMax(Double.parseDouble(sqlResult[index++]));
                    break;
                case Date:
                    ((DateColumn) column).setBegin(sqlResult[index++]);
                    ((DateColumn) column).setEnd(sqlResult[index++]);
                    break;
                case Bool:
                    break;
                default:
                    throw new Exception("未匹配到的类型");
            }
        }
    }

    @Override
    public void setDataRangeUnique(Schema schema, AbstractDbConnector dbConnector)
            throws IOException, SQLException {
        TidbStatsJsonObject tidbStatsJsonObject = JSON.parseObject(((TidbConnector) dbConnector).
                tableInfoJson(schema.getTableName()).
                replace(" ", ""), TidbStatsJsonObject.class);
        schema.setTableSize(tidbStatsJsonObject.getCount());
        for (AbstractColumn column : schema.getNotKeyColumns()) {
            column.setNullPercentage(tidbStatsJsonObject.getNullProbability(column.getColumnName()));
            if (column.getColumnType() == ColumnType.Int) {
                ((IntColumn) column).setNdv(tidbStatsJsonObject.getNdv(column.getColumnName()));
            } else if (column.getColumnType() == ColumnType.String) {
                ((StringColumn) column).setNdv(tidbStatsJsonObject.getNdv(column.getColumnName()));
                ((StringColumn) column).setAvgLength(tidbStatsJsonObject.getAvgLength(column.getColumnName()));
            }
        }
    }
}
