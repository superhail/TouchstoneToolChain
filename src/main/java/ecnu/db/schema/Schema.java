package ecnu.db.schema;

import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.schema.column.AbstractColumn;
import ecnu.db.schema.column.ColumnType;
import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

/**
 * @author wangqingshuai
 */
public class Schema {
    private final String tableName;
    private final HashMap<String, AbstractColumn> columns;
    private int tableSize;
    private String primaryKeys;
    private HashMap<String, String> foreignKeys;
    private HashMap<String, String> metaDataFks; // 根据Database的metadata获取的外键信息
    private int joinTag;
    private int lastJoinTag;

    public Schema(String tableName, HashMap<String, AbstractColumn> columns) {
        this.tableName = tableName;
        this.columns = columns;
        joinTag = 1;
        lastJoinTag = 1;
    }

    public HashMap<String, AbstractColumn> getColumns() {
        return columns;
    }

    // 初始化Schema.foreignKeys和Schema.metaDataFks
    public static void initFks(DatabaseMetaData metaData, HashMap<String, Schema> schemas) throws SQLException, TouchstoneToolChainException {
        for (Map.Entry<String, Schema> entry: schemas.entrySet()) {
            String tableName = entry.getKey();
            ResultSet rs = metaData.getImportedKeys(null, null, tableName);
            while(rs.next()) {
                String pkTable = rs.getString("PKTABLE_NAME"), pkCol = rs.getString("PKCOLUMN_NAME"),
                        fkTable = rs.getString("FKTABLE_NAME"), fkCol = rs.getString("FKCOLUMN_NAME");
                schemas.get(fkTable).addForeignKey(fkCol, pkTable, pkCol);
            }
        }

        for (Map.Entry<String, Schema> entry: schemas.entrySet()) {
            Schema schema = entry.getValue();
            HashMap<String, String> fks = Optional.ofNullable(schema.getForeignKeys()).orElse(new HashMap<>());
            schema.setMetaDataFks(fks);
        }
    }

    public int getJoinTag() {
        int temp = joinTag;
        joinTag *= 4;
        return temp;
    }

    public void keepJoinTag(boolean keep) {
        if (keep) {
            lastJoinTag = joinTag;
        } else {
            joinTag = lastJoinTag;
        }
    }

    public void addForeignKey(String localColumnName, String referencingTable, String referencingInfo) throws TouchstoneToolChainException {
        if (foreignKeys == null) {
            foreignKeys = new HashMap<>();
        }
        String[] columnNames = localColumnName.split(",");
        String[] refColumnNames = referencingInfo.split(",");
        for (int i = 0; i < columnNames.length; i++) {
            if (foreignKeys.containsKey(columnNames[i])) {
                if (!(referencingTable + "." + refColumnNames[i]).equals(foreignKeys.get(columnNames[i]))) {
                    throw new TouchstoneToolChainException("冲突的主外键连接");
                } else {
                    return;
                }
            }
            foreignKeys.put(columnNames[i], referencingTable + "." + refColumnNames[i]);
        }

    }

    public void setPrimaryKeys(String primaryKeys) throws TouchstoneToolChainException {
        if (this.primaryKeys == null) {
            this.primaryKeys = primaryKeys;
        } else {
            HashSet<String> newKeys = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
            HashSet<String> keys = new HashSet<>(Arrays.asList(this.primaryKeys.split(",")));
            if (keys.size() == newKeys.size()) {
                keys.removeAll(newKeys);
                if (keys.size() > 0) {
                    throw new TouchstoneToolChainException("query中使用了多列主键的部分主键");
                }
            } else {
                throw new TouchstoneToolChainException("query中使用了多列主键的部分主键");
            }
        }
    }

    public int getNdv(String columnName) throws TouchstoneToolChainException {
        if (!columns.containsKey(columnName)) {
            throw new TouchstoneToolChainException("不存在的列" + columnName);
        }
        return columns.get(columnName).getNdv();
    }

    public Collection<AbstractColumn> getAllColumns() {
        return columns.values();
    }


    /**
     * 格式化返回schema信息
     *
     * @return 返回满足touchstone格式的schema信息
     */
    public String formatSchemaInfo() {
        if (primaryKeys == null && foreignKeys == null) {
            return null;
        }
        int k = 1;
        if (tableSize < 100) {
            k = 100;
        }
        StringBuilder schemaInfo = new StringBuilder("T[" + tableName + ';' + tableSize * k + ';');
        List<String> hasProduct = new ArrayList<>();
        for (AbstractColumn column : columns.values()) {
            if (primaryKeys != null) {
                if (primaryKeys.contains(column.getColumnName())) {
                    schemaInfo.append(column.formatColumnType());
                    hasProduct.add(column.getColumnName());
                }
            }
        }

        for (AbstractColumn column : columns.values()) {
            if (!hasProduct.contains(column.getColumnName())) {
                if (foreignKeys != null) {
                    if (foreignKeys.containsKey(column.getColumnName())) {
                        schemaInfo.append(column.formatColumnType());
                        hasProduct.add(column.getColumnName());
                    }
                }
            }
        }

        for (AbstractColumn column : columns.values()) {
            if (!hasProduct.contains(column.getColumnName())) {
                schemaInfo.append(column.formatColumnType());
            }
        }

        if (primaryKeys != null) {
            schemaInfo.append("P(").append(primaryKeys).append(");");
        } else {
            schemaInfo.append("P(");
            for (String localKey : foreignKeys.keySet()) {
                schemaInfo.append(localKey).append(",");
            }
            schemaInfo.replace(schemaInfo.length(), schemaInfo.length(), ");");
        }

        if (foreignKeys != null) {
            for (Map.Entry<String, String> keyAndRefKey : foreignKeys.entrySet()) {
                schemaInfo.append("F(").append(keyAndRefKey.getKey()).append(',').
                        append(keyAndRefKey.getValue()).append(");");
            }
        }
        return schemaInfo.replace(schemaInfo.length() - 1, schemaInfo.length(), "]").toString();
    }

    /**
     * 格式化返回数据分布信息
     *
     * @return 返回满足touchstone格式的数据分布
     */
    public String formatDataDistributionInfo() throws ParseException {
        if (primaryKeys == null && foreignKeys == null) {
            return null;
        }
        StringBuilder dataDistributionInfo = new StringBuilder();
        for (AbstractColumn column : columns.values()) {
            boolean skip = false;
            if (primaryKeys != null && primaryKeys.contains(column.getColumnName())) {
                skip = true;
            }
            if (foreignKeys != null && foreignKeys.containsKey(column.getColumnName())) {
                skip = true;
            }
            if (!skip) {
                dataDistributionInfo.append("D[").append(tableName).append(".").
                        append(column.formatDataDistribution()).append("]\n");
            }
        }
        return dataDistributionInfo.toString();
    }

    public boolean isDate(String columnName) {
        return columns.get(columnName).getColumnType() == ColumnType.DATETIME;
    }

    public String getTableName() {
        return tableName;
    }

    public int getTableSize() {
        return tableSize;
    }

    public void setTableSize(int tableSize) {
        this.tableSize = tableSize;
    }

    public HashMap<String, String> getMetaDataFks() {
        return metaDataFks;
    }

    public void setMetaDataFks(HashMap<String, String> metaDataFks) {
        this.metaDataFks = metaDataFks;
    }

    public HashMap<String, String> getForeignKeys() {
        return foreignKeys;
    }

    @Override
    public String toString() {
        return "Schema{" +
                "tableName='" + tableName + '\'' +
                ", tableSize=" + tableSize +
                '}';
    }
}
