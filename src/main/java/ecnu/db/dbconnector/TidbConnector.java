package ecnu.db.dbconnector;

import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class TidbConnector extends AbstractDbConnector {
    String statsUrl;

    public TidbConnector(SystemConfig config) throws TouchstoneToolChainException {
        super(config);
        statsUrl = "http://" + config.getDatabaseIp() + ':' +
                config.getTidbHttpPort() + "/stats/dump/" + config.getDatabaseName() + "/";
    }

    @Override
    String dbUrl(SystemConfig config) {
        return "jdbc:mysql://" +
                config.getDatabaseIp() + ":" +
                config.getDatabasePort() + "/" +
                config.getDatabaseName() +
                "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
    }

    @Override
    String abstractGetTableNames() {
        return "show tables;";
    }

    @Override
    String abstractGetCreateTableSql(String tableName) {
        return "show create table " + tableName;
    }

    public String tableInfoJson(String tableName) throws IOException, SQLException {
        //stmt.executeQuery("analyze table " + tableName);
        InputStream is = new URL(statsUrl + tableName).openStream();
        BufferedReader rd = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        String line;
        StringBuilder json = new StringBuilder();
        while ((line = rd.readLine()) != null) {
            json.append(line);
        }
        is.close();
        return json.toString();
    }
}
