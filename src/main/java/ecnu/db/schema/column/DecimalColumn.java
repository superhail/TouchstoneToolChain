package ecnu.db.schema.column;

public class DecimalColumn extends AbstractColumn {
    double min;
    double max;

    public DecimalColumn(String columnName) {
        super(columnName, ColumnType.Decimal);
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + ',' + min + ',' + max;
    }
}
