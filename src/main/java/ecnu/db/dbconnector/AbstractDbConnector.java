package ecnu.db.dbconnector;

import ecnu.db.utils.SystemConfig;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * @author wangqingshuai
 * 数据库驱动连接器
 */
public abstract class AbstractDbConnector {
    /**
     * JDBC 驱动名及数据库 URL
     */
    protected Statement stmt;

    AbstractDbConnector(SystemConfig config) {
        // 数据库的用户名与密码
        String user = config.getDatabaseUser();
        String pass = config.getDatabasePwd();
        try {
            stmt = DriverManager.getConnection(dbUrl(config), user, pass).createStatement();
        } catch (SQLException e) {
            System.out.println(dbUrl(config));
            System.out.println("无法建立数据库连接");
            System.exit(-1);
        }
    }

    abstract String dbUrl(SystemConfig config);

    abstract String abstractGetTableNames();

    abstract String abstractGetCreateTableSql(String tableName);

    abstract String abstractExplainQuery(String sql);

    //数据库标准操作
    public ArrayList<String> getTableNames() throws SQLException {
        ResultSet rs = stmt.executeQuery(abstractGetTableNames());
        ArrayList<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(1).trim().toLowerCase());
        }
        return tables;
    }

    public String getCreateTableSql(String tableName) throws SQLException {
        ResultSet rs = stmt.executeQuery(abstractGetCreateTableSql(tableName));
        rs.next();
        return rs.getString(2).trim().toLowerCase();
    }

    public String[] getDataRange(String tableName, String columnInfo) throws SQLException {
        String sql = "select " + columnInfo + " from " + tableName;
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        String[] infos = new String[rs.getMetaData().getColumnCount()];
        for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            infos[i - 1] = rs.getString(i).trim().toLowerCase();
        }
        return infos;
    }

    public ArrayList<String[]> explainQuery(String sql, String[] sqlInfoColumns) throws SQLException {
        ResultSet rs = stmt.executeQuery(abstractExplainQuery(sql));
        ArrayList<String[]> result = new ArrayList<>();
        while (rs.next()) {
            String[] infos = new String[sqlInfoColumns.length];
            for (int i = 0; i < sqlInfoColumns.length; i++) {
                infos[i] = rs.getString(sqlInfoColumns[i]);
            }
            result.add(infos);
        }
        return result;
    }

}
