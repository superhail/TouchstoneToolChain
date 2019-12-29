package ecnu.db.schema.column;


import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;


public class DateColumn extends AbstractColumn {
    private String begin;
    private String end;

    public DateColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    public void setBegin(String begin) {
        this.begin = begin;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    public String formatDataDistribution() throws ParseException {
        SimpleDateFormat touchstoneFmt=new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
        String[] pattern = new String[]{"yyyy-MM", "yyyyMM", "yyyy/MM", "yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd",
                "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss"};
        return columnName + ";" + nullPercentage + ';' + touchstoneFmt.format(DateUtils.parseDate(begin, pattern))  + ";" +
                touchstoneFmt.format(DateUtils.parseDate(end, pattern).getTime());
    }
}
