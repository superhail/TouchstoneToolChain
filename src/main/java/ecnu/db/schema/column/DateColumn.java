package ecnu.db.schema.column;

public class DateColumn extends AbstractColumn {
    private String begin;
    private String end;

    public DateColumn(String columnName) {
        super(columnName, ColumnType.Date);
    }

    public void setBegin(String begin) {
        this.begin = begin;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    @Override
    public String formatDataDistribution() {
        return columnName + ";" + nullPercentage + ';' + begin + ";" + end;
    }
}
