package ecnu.db.dbconnector;

import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.*;
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

    public DatabaseMetaData databaseMetaData;

    AbstractDbConnector(SystemConfig config) throws TouchstoneToolChainException {
        // 数据库的用户名与密码
        String user = config.getDatabaseUser();
        String pass = config.getDatabasePwd();
        try {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            stmt = DriverManager.getConnection(dbUrl(config), user, pass).createStatement();
            databaseMetaData = DriverManager.getConnection(dbUrl(config), user, pass).getMetaData();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            throw new TouchstoneToolChainException("无法建立数据库连接,连接信息为\n" + dbUrl(config));
        }
    }

    abstract String dbUrl(SystemConfig config);

    abstract String abstractGetTableNames();

    abstract String abstractGetCreateTableSql(String tableName);

    public ArrayList<String> getTableNames() throws SQLException {
        ResultSet rs = stmt.executeQuery(abstractGetTableNames());
        ArrayList<String> tables = new ArrayList<>();
        while (rs.next()) {
            tables.add(rs.getString(1).trim().toLowerCase());
        }
        return tables;
    }

    public String getTableDDL(String tableName) throws SQLException {
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
            try {
                infos[i - 1] = rs.getString(i).trim().toLowerCase();
            } catch (NullPointerException e) {
                infos[i - 1] = "0";
            }
        }
        return infos;
    }

    public ArrayList<String[]> explainQuery(String sql, String[] sqlInfoColumns) throws SQLException {
        ResultSet rs = stmt.executeQuery("explain analyze " + sql);
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

    public int getMultiColumnsNdv(String schema, String columns) throws SQLException {
        ResultSet rs = stmt.executeQuery("select count(distinct " + columns + ") from " + schema);
        rs.next();
        return rs.getInt(1);
    }


}
