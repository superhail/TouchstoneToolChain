package ecnu.db.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;

public class TidbStatsJsonObject {
    int count;
    HashMap<String, Distribution> columns;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public HashMap<String, Distribution> getColumns() {
        return columns;
    }

    public void setColumns(HashMap<String, Distribution> columns) {
        this.columns = columns;
    }

    public float getNullProbability(String name) {
        return (float) columns.get(name).null_count / count;
    }

    public BigDecimal getAvgLength(String name) {
        if (count == 0) {
            return new BigDecimal(0);
        }
        BigDecimal totalSize = BigDecimal.valueOf(columns.get(name).tot_col_size);
        BigDecimal tableSize = BigDecimal.valueOf(count);
        return totalSize.divide(tableSize, 4, RoundingMode.HALF_UP).subtract(BigDecimal.valueOf(2));
    }


    public int getNdv(String name) {
        return columns.get(name).getHistogram().getNdv();
    }
}

class Distribution {
    int null_count;
    Histogram histogram;
    int tot_col_size;

    public int getTot_col_size() {
        return tot_col_size;
    }

    public void setTot_col_size(int tot_col_size) {
        this.tot_col_size = tot_col_size;
    }

    public Histogram getHistogram() {
        return histogram;
    }

    public void setHistogram(Histogram histogram) {
        this.histogram = histogram;
    }

    public int getNull_count() {
        return null_count;
    }

    public void setNull_count(int null_count) {
        this.null_count = null_count;
    }
}

class Histogram {
    int ndv;

    public int getNdv() {
        return ndv;
    }

    public void setNdv(int ndv) {
        this.ndv = ndv;
    }
}


