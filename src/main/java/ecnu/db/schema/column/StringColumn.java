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

    public BigDecimal getAvgLength() {
        return avgLength;
    }

    public int getMaxLength() {
        return maxLength;
    }

    @Override
    public int getNdv() {
        return this.ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    @Override
    public String formatDataDistribution() {
        // TODO
        return columnName + ";" + nullPercentage + ';' + avgLength + ';' + maxLength + ';' + ndv;
    }
}
