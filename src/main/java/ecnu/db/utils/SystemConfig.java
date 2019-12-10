package ecnu.db.utils;

import com.alibaba.fastjson.JSON;
import ecnu.db.schema.column.ColumnType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

public class SystemConfig {

    private String databaseIp;
    private String databasePort;
    private String databaseUser;
    private String databasePwd;
    private String databaseName;
    private String tidbHttpPort;
    private String databaseVersion;
    private HashMap<HashSet<String>, ColumnType> typeConvert;

    public SystemConfig() {
        databaseIp = "127.0.0.1";
        databaseUser = "root";
        databasePwd = "";
        databasePort = "4000";
        databaseName = "tpch";
    }

    public static SystemConfig readConfig(String filePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(filePath));
        StringBuilder configJson = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            configJson.append(line);
        }
        SystemConfig systemConfig = JSON.parseObject(configJson.toString(), SystemConfig.class);
        ConfigConvert.setTypeConvert(systemConfig.getTypeConvert());
        return systemConfig;
    }

    public String getDatabaseVersion() {
        return databaseVersion;
    }

    public void setDatabaseVersion(String databaseVersion) {
        this.databaseVersion = databaseVersion;
    }

    public String getTidbHttpPort() {
        return tidbHttpPort;
    }

    public void setTidbHttpPort(String tidbHttpPort) {
        this.tidbHttpPort = tidbHttpPort;
    }

    public HashMap<HashSet<String>, ColumnType> getTypeConvert() {
        return typeConvert;
    }

    public void setTypeConvert(HashMap<HashSet<String>, ColumnType> typeConvert) {
        this.typeConvert = typeConvert;
    }

    public ColumnType getColumnType(String readType) {
        return ColumnType.Int;
    }

    public String getDatabaseIp() {
        return databaseIp;
    }

    public void setDatabaseIp(String databaseIp) {
        this.databaseIp = databaseIp;
    }

    public String getDatabasePort() {
        return databasePort;
    }

    public void setDatabasePort(String databasePort) {
        this.databasePort = databasePort;
    }

    public String getDatabaseUser() {
        return databaseUser;
    }

    public void setDatabaseUser(String databaseUser) {
        this.databaseUser = databaseUser;
    }

    public String getDatabasePwd() {
        return databasePwd;
    }

    public void setDatabasePwd(String databasePwd) {
        this.databasePwd = databasePwd;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }
}
