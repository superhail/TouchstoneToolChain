package ecnu.db.schema.column;


public abstract class AbstractColumn {
    protected float nullPercentage;
    protected String columnName;
    private ColumnType columnType;

    public AbstractColumn(String columnName, ColumnType columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    public String getColumnName() {
        return columnName;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setNullPercentage(float nullPercentage) {
        this.nullPercentage = nullPercentage;
    }

    public String formatColumnType() {
        return columnName + ',' + columnType + ';';
    }

    public abstract String formatDataDistribution();
}
