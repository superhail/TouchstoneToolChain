package ecnu.db.dbconnector;

import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author lianxuechao
 */
public interface DatabaseConnectorInterface {
    List<String> getTableNames() throws SQLException;

    List<String[]> explainQuery(String queryCanonicalName, String sql, String[] sqlInfoColumns) throws SQLException, TouchstoneToolChainException;

    int getMultiColNdv(String schema, String columns) throws SQLException, TouchstoneToolChainException;

    Map<String, Integer> getMultiColNdvMap();
}
