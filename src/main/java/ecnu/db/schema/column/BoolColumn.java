package ecnu.db.schema.column;

import java.math.BigDecimal;

public class BoolColumn extends AbstractColumn {
    private BigDecimal trueProbability;

    public BoolColumn(String columnName) {
        super(columnName, ColumnType.Bool);
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + "," + trueProbability;
    }
}
