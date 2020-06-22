package ecnu.db.analyzer.online;

/**
 * @author lianxuechao
 */
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

    public void addConstraint(String data) {
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

    @Override
    public String toString() {
        return "QueryInfo{" +
                "data='" + data + '\'' +
                ", tableName='" + tableName + '\'' +
                ", stop=" + stop +
                '}';
    }
}
