package ecnu.db.schema.column;


import com.alibaba.fastjson.annotation.JSONType;

import java.text.ParseException;
@JSONType(serializer = ColumnSerializer.class, deserializer = ColumnDeserializer.class)
public abstract class AbstractColumn {
    private final ColumnType columnType;
    protected float nullPercentage;
    protected String columnName;

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public abstract int getNdv();

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    public float getNullPercentage() {
        return nullPercentage;
    }

    public String formatColumnType() {
        return columnName + ',' + columnType + ';';
    }

    public abstract String formatDataDistribution() throws ParseException;
}
