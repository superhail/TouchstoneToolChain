package ecnu.db.schema.column;

public class IntColumn extends AbstractColumn {
    private int min;
    private int max;
    private int ndv;

    public IntColumn(String columnName) {
        super(columnName, ColumnType.INTEGER);
    }

    public void setMin(int min) {
        this.min = min;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + ';' + ndv + ';' + min + ';' + max;
    }
}
