package ecnu.db.schema.column;

import java.math.BigDecimal;

public class StringColumn extends AbstractColumn {
    private BigDecimal avgLength;
    private int maxLength;
    private int ndv;

    public StringColumn(String columnName) {
        super(columnName, ColumnType.VARCHAR);
    }

    public void setAvgLength(BigDecimal avgLength) {
        this.avgLength = avgLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
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
        return columnName + ";" + nullPercentage + ';' + avgLength + ';' + maxLength + ';' + ndv;
    }
}
