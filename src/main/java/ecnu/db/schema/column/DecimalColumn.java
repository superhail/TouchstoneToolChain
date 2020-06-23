package ecnu.db.schema.column;

public class DecimalColumn extends AbstractColumn {
    double min;
    double max;

    public DecimalColumn(String columnName) {
        super(columnName, ColumnType.DECIMAL);
    }

    public void setMin(double min) {
        this.min = min;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + ';' + min + ';' + max;
    }
}
