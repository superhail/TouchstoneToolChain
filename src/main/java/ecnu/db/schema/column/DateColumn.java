package ecnu.db.schema.column;


import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;


public class DateColumn extends AbstractColumn {
    private static final SimpleDateFormat touchstoneFmt = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
    private static final String[] dataTimePattern = new String[]{"yyyy-MM", "yyyyMM", "yyyy/MM", "yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd",
            "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss"};
    private static final Pattern pattern = Pattern.compile("-?\\d+(\\.\\d+)?");
    private String begin;
    private String end;

    public DateColumn(String columnName) {
        super(columnName, ColumnType.DATETIME);
    }

    public void setBegin(String begin) {
        this.begin = begin;
    }

    public String getBegin() {
        return begin;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public String getEnd() {
        return end;
    }

    @Override
    public int getNdv() {
        return -1;
    }

    @Override
    public String formatDataDistribution() throws ParseException {

        if (isNumeric(begin)) {
            return columnName + ";" + nullPercentage + ';' + touchstoneFmt.format(Long.parseLong(begin)) + ";" +
                    touchstoneFmt.format(Long.parseLong(end));
        } else {
            return columnName + ";" + nullPercentage + ';' +
                    touchstoneFmt.format(DateUtils.parseDate(begin, dataTimePattern)) + ";" +
                    touchstoneFmt.format(DateUtils.parseDate(end, dataTimePattern).getTime());
        }
    }

    private boolean isNumeric(String strNum) {
        if (strNum == null) {
            return false;
        }
        return pattern.matcher(strNum).matches();
    }
}
