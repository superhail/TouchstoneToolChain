package ecnu.db.schema;

import ecnu.db.schema.column.AbstractColumn;

import java.util.*;

/**
 * @author wangqingshuai
 */
public class Schema {
    private String tableName;
    private int tableSize;
    private ArrayList<String> primaryKeys;
    private HashMap<String, String> foreignKeys;
    private ArrayList<AbstractColumn> columns;
    private HashSet<String> columnNames;


    public Schema(String tableName, ArrayList<String> primaryKeys, HashMap<String, String> foreignKeys, ArrayList<AbstractColumn> columns) {
        this.tableName = tableName;
        this.primaryKeys = primaryKeys;
        this.foreignKeys = foreignKeys;
        this.columns = columns;
        this.columnNames = new HashSet<>();
        for (AbstractColumn column : columns) {
            columnNames.add(column.getColumnName());
        }
    }

    /**
     * 判定给定的表格是否满足全局拓扑序
     *
     * @param schemas 待验证的表格
     * @return 是否存在全局拓扑序
     */
    public static boolean existsTopologicalOrderOrNot(Collection<Schema> schemas) {
        HashSet<String> topologicalTables = new HashSet<>();
        for (int i = 0; i < schemas.size(); i++) {
            for (Schema schema : schemas) {
                if (!topologicalTables.contains(schema.getTableName()) && schema.onlyReferencingTables(topologicalTables)) {
                    topologicalTables.add(schema.getTableName());
                    break;
                }
            }
            if (i == topologicalTables.size()) {
                return false;
            }
        }
        return true;
    }

    public void addForeignKey(String columnName, String referencingTableInfo) throws Exception {
        if (foreignKeys == null) {
            foreignKeys = new HashMap<>();
        }
        if (foreignKeys.containsKey(columnName)) {
            if (!referencingTableInfo.equals(foreignKeys.get(columnName))) {
                throw new Exception("冲突的主外键连接");
            } else {
                return;
            }
        }
        foreignKeys.put(columnName, referencingTableInfo);
    }

    /**
     * 判断本表是否只依赖于这些表，用于确定是否存在全局拓扑序
     *
     * @param tableNames 已经确定存在拓扑序的表格
     * @return 是否只依赖于这些表
     */
    public boolean onlyReferencingTables(HashSet<String> tableNames) {
        if (foreignKeys != null) {
            for (String referencingTableInfo : foreignKeys.values()) {
                if (!tableNames.contains(referencingTableInfo.split(".")[0])) {
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isColumn(String name) {
        return columnNames.contains(name);
    }

    public boolean isPrimaryKey(String name) {
        return primaryKeys.contains(name);
    }

    public int getPrimaryKeySize(){
        return primaryKeys.size();
    }


    /**
     * 获取既不是主键也不是外键的列
     *
     * @return 满足要求的列
     */
    public ArrayList<AbstractColumn> getNotKeyColumns() {
        ArrayList<AbstractColumn> results = new ArrayList<>();
        for (AbstractColumn column : columns) {
            if (!primaryKeys.contains(column.getColumnName()) && !foreignKeys.containsKey(column.getColumnName())) {
                results.add(column);
            }
        }
        return results;
    }

    /**
     * 格式化返回schema信息
     *
     * @return 返回满足touchstone格式的schema信息
     */
    public String formatSchemaInfo() {
        StringBuilder schemaInfo = new StringBuilder("T[" + tableName + ';' + tableSize + ';');
        for (AbstractColumn column : columns) {
            schemaInfo.append(column.formatColumnType());
        }
        schemaInfo.append("P(");
        for (String primaryKey : primaryKeys) {
            schemaInfo.append(primaryKey).append(',');
        }
        schemaInfo.replace(schemaInfo.length() - 1, schemaInfo.length() - 1, ");");
        if (foreignKeys != null) {
            for (Map.Entry<String, String> keyAndRefKey : foreignKeys.entrySet()) {
                schemaInfo.append("F(").append(keyAndRefKey.getKey()).append(',').
                        append(keyAndRefKey.getValue()).append(");");
            }
        }
        return schemaInfo.replace(schemaInfo.length() - 1, schemaInfo.length() - 1, "]").toString();
    }

    /**
     * 格式化返回数据分布信息
     *
     * @return 返回满足touchstone格式的数据分布
     */
    public String formatDataDistributionInfo() {
        StringBuilder dataDistributionInfo = new StringBuilder();
        for (AbstractColumn column : columns) {
            if (!primaryKeys.contains(column.getColumnName()) && !foreignKeys.containsKey(column.getColumnName())) {
                continue;
            }
            dataDistributionInfo.append("D[").append(tableName).append(".").append(column.formatDataDistribution()).append("]\n");
        }
        return dataDistributionInfo.toString();
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public String getTableName() {
        return tableName;
    }
}
