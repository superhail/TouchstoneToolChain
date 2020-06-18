package ecnu.db.analyzer.online;

public class QueryInfo {
    String data;
    String tableName;
    int lastNodeLineCount;
    boolean stop;

    public QueryInfo(String data, String tableName, int lastNodeLineCount) {
        this.data = data;
        this.lastNodeLineCount = lastNodeLineCount;
        this.tableName = tableName;
        stop = false;

    }

    public String getTableName() {
        return tableName;
    }

    public String getData() {
        return data;
    }

    public void addQueryInfo(String data) {
        this.data += data;
    }

    public int getLastNodeLineCount() {
        return lastNodeLineCount;
    }

    public void setLastNodeLineCount(int lastNodeLineCount) {
        this.lastNodeLineCount = lastNodeLineCount;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop() {
        this.stop = true;
    }
}
