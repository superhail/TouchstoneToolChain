package ecnu.db.query.ccoutput;

public class QueryInfoState {
    String queryInfo;
    String tableName;
    int lastNodeLineCount;
    boolean stop;
    boolean fullTable;

    public QueryInfoState(String queryInfo, String tableName, int lastNodeLineCount, boolean fullTable) {
        this.fullTable = fullTable;
        this.queryInfo = queryInfo;
        this.lastNodeLineCount = lastNodeLineCount;
        this.tableName = tableName;
        stop = false;

    }

    public QueryInfoState() {
        queryInfo = "";
        tableName = "";
        lastNodeLineCount = -1;
        stop = true;
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

    public boolean isFullTable() {
        return fullTable;
    }

    public void setFullTable() {
        this.fullTable = false;
    }
}
