package ecnu.db.schema.column;

import java.math.BigDecimal;

public class StringColumn extends AbstractColumn {
    private BigDecimal avgLength;
    private int maxLength;
    private int ndv;

    public StringColumn(String columnName) {
        super(columnName, ColumnType.String);
    }

    public void setAvgLength(BigDecimal avgLength) {
        this.avgLength = avgLength;
    }

    public void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + ';' + ndv + ';' + avgLength + ';' + maxLength;
    }
}
