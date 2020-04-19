package ecnu.db.analyzer.online;

public class QueryInfoChain {
    String queryInfo;
    String tableName;
    int lastNodeLineCount;
    boolean stop;

    public QueryInfoChain(String queryInfo, String tableName, int lastNodeLineCount) {
        this.queryInfo = queryInfo;
        this.lastNodeLineCount = lastNodeLineCount;
        this.tableName = tableName;
        stop = false;

    }

    public String getTableName() {
        return tableName;
    }

    public String getQueryInfo() {
        return queryInfo;
    }

    public void addQueryInfo(String queryInfo) {
        this.queryInfo += queryInfo;
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
