package ecnu.db.dbconnector;

import ecnu.db.utils.TouchstoneToolChainException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface DatabaseConnectorInterface {
    public List<String> getTableNames() throws SQLException;
    public List<String[]> explainQuery(String queryCanonicalName, String sql, String[] sqlInfoColumns) throws SQLException, TouchstoneToolChainException;
    public int getMultiColNdv(String schema, String columns) throws SQLException, TouchstoneToolChainException;
    public Map<String, Integer> getMultiColNdvMap();
}
