package ecnu.db;


import com.alibaba.druid.sql.SQLUtils;
import ecnu.db.analyzer.online.AbstractAnalyzer;
import ecnu.db.analyzer.online.ExecutionNode;
import ecnu.db.analyzer.online.Tidb3Analyzer;
import ecnu.db.analyzer.statical.QueryTableName;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.schema.Schema;
import ecnu.db.schema.generation.AbstractSchemaGenerator;
import ecnu.db.schema.generation.TidbSchemaGenerator;
import ecnu.db.utils.ReadQuery;
import ecnu.db.utils.SystemConfig;
import ecnu.db.utils.TouchstoneToolChainException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.*;

public class Main {

    public static String changeSql(String sql, HashMap<String, List<String>> argsAndIndex, ArrayList<String> cannotFindArgs,
                                   ArrayList<String> conflictArgs) throws TouchstoneToolChainException {
        HashSet<String> stopSqlSwap = new HashSet<>(Arrays.asList("and", "limit", "group", ")"));
        for (Map.Entry<String, List<String>> argAndIndexes : argsAndIndex.entrySet()) {
            if (argAndIndexes.getKey().contains("in(")) {
                int lastIndex = 0;
                int count = 0;
                while (lastIndex != -1) {
                    lastIndex = sql.indexOf("in (", lastIndex);
                    if (lastIndex != -1) {
                        count++;
                        lastIndex += argAndIndexes.getKey().length();
                    }
                }
                if (count > 1) {
                    conflictArgs.add(argAndIndexes.getKey());
                } else {
                    String backString = sql.substring(sql.indexOf("in (") + "in (".length());
                    backString = backString.substring(backString.indexOf(")") + 1);
                    sql = sql.substring(0, sql.indexOf("in (") + "in ".length()) + argAndIndexes.getValue().get(0) + backString;
                }
            } else {
                int lastIndex = 0;
                int count = 0;
                while (lastIndex != -1) {
                    lastIndex = sql.indexOf(argAndIndexes.getKey(), lastIndex);
                    if (lastIndex != -1) {
                        count++;
                        lastIndex += argAndIndexes.getKey().length();
                    }
                }
                if (count == 0) {
                    cannotFindArgs.add(argAndIndexes.getKey());
                } else if (count == 1) {
                    int front = sql.indexOf(argAndIndexes.getKey()) + argAndIndexes.getKey().length();
                    StringBuilder backString = new StringBuilder(sql.substring(front + 1));
                    String[] sqlTuples = backString.toString().split(" ");
                    int i = 0;
                    boolean hasBetween = false;
                    if (argAndIndexes.getKey().contains(" bet")) {
                        hasBetween = true;
                    }
                    for (; i < sqlTuples.length; i++) {
                        if (!hasBetween) {
                            if (stopSqlSwap.contains(sqlTuples[i].toLowerCase()) || sqlTuples[i].contains(";")) {
                                break;
                            }
                        } else {
                            if ("and".equals(sqlTuples[i])) {
                                hasBetween = false;
                            }
                        }
                    }
                    if (i < sqlTuples.length) {
                        backString = new StringBuilder();
                        if (sqlTuples[i].contains(";")) {
                            backString.append(";");
                        } else {
                            for (; i < sqlTuples.length; i++) {
                                backString.append(" ").append(sqlTuples[i]);
                            }
                        }
                        if (argAndIndexes.getKey().contains(" bet")) {
                            sql = sql.substring(0, front) + argAndIndexes.getValue().get(0) + backString.toString();
                        } else {
                            sql = sql.substring(0, front + 1) + "'" + argAndIndexes.getValue().get(0) + "'" + backString.toString();
                        }

                    } else {
                        throw new TouchstoneToolChainException("检测不到停止的语法词");
                    }
                } else if (count > 1) {
                    conflictArgs.add(argAndIndexes.getKey());
                }
            }
        }
        return sql;
    }

    public static void main(String[] args) throws TouchstoneToolChainException, SQLException, IOException, ParseException {
        SystemConfig systemConfig = SystemConfig.readConfig(args[0]);

        File sqlInput = new File(systemConfig.getSqlsDirectory());
        File[] files = sqlInput.listFiles();
        if (files == null) {
            throw new TouchstoneToolChainException("该文件夹下没有sql文件");
        }

        File writeDirectory = new File(systemConfig.getResultDirectory());

        AbstractDbConnector dbConnector = new TidbConnector(systemConfig);
        AbstractSchemaGenerator dbSchemaGenerator = new TidbSchemaGenerator();

        System.out.println("开始获取表名");
        ArrayList<String> tableNames;
        if (systemConfig.isCrossMultiDatabase()) {
            HashSet<String> tableNameSet = new HashSet<>();
            for (File sqlFile : files) {
                if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                    List<String> sqls = ReadQuery.getSQLsFromFile(sqlFile.getPath(), "mysql");
                    for (String sql : sqls) {
                        tableNameSet.addAll(QueryTableName.getTableName(sql, "mysql"));
                    }
                }
            }
            tableNames = new ArrayList<>(tableNameSet);
        } else {
            tableNames = dbConnector.getTableNames();
        }
        System.out.println("获取表名成功，表名为:" + tableNames);
        HashMap<String, Schema> schemas = new HashMap<>(tableNames.size());
        System.out.println("开始获取表结构和数据分布");
        for (String tableName : tableNames) {
            System.out.println("开始获取" + tableName + "的信息");
            System.out.print("获取表结构...");
            Schema schema = dbSchemaGenerator.generateSchemaNoKeys(tableName, dbConnector.getTableDDL(tableName));
            System.out.println("成功");
            System.out.print("获取表数据分布...");
            dbSchemaGenerator.setDataRangeBySqlResult(schema.getAllColumns(), dbConnector.getDataRange(tableName,
                    dbSchemaGenerator.getColumnDistributionSql(schema.getTableName(), schema.getAllColumns())));
            dbSchemaGenerator.setDataRangeUnique(schema, dbConnector);
            System.out.println("成功");
            schemas.put(tableName, schema);
        }
        Schema.initFks(dbConnector.databaseMetaData, schemas);

        System.out.println("获取表结构和数据分布成功，开始获取query查询计划");

        AbstractAnalyzer queryAnalyzer = new Tidb3Analyzer(dbConnector, systemConfig.getTidbSelectArgs(), schemas);

        File retDir = new File(systemConfig.getResultDirectory()), retSqlDir = new File(systemConfig.getResultDirectory() + "/sql/");
        retDir.mkdir();
        retSqlDir.mkdir();

        List<String> queryInfos = new LinkedList<>();
        for (File sqlFile : files) {
            if (sqlFile.isFile() && sqlFile.getName().endsWith(".sql")) {
                List<String> sqls = ReadQuery.getSQLsFromFile(sqlFile.getPath(), queryAnalyzer.getDbType());
                int index = 0;
                BufferedWriter sqlWriter = new BufferedWriter(new FileWriter(
                        new File(retSqlDir.getPath() + "/" + sqlFile.getName())));
                List<String[]> queryPlan = new ArrayList<>();
                for (String sql : sqls) {
                    index++;
                    String queryCanonicalName = String.format("%s_%d", sqlFile.getName(), index);
                    try {
                        queryInfos.add("## " + queryCanonicalName);
                        queryPlan = queryAnalyzer.getQueryPlan(sql);
                        ExecutionNode root = queryAnalyzer.getExecutionTree(queryPlan);
                        queryInfos.addAll(queryAnalyzer.extractQueryInfos(root));
                        String outputMessage = String.format("%-15s Status:\033[32m获取成功\033[0m", queryCanonicalName);
                        queryAnalyzer.outputSuccess(true);

                        ArrayList<String> cannotFindArgs = new ArrayList<>();
                        ArrayList<String> conflictArgs = new ArrayList<>();
                        sql = changeSql(sql, queryAnalyzer.getArgsAndIndex(), cannotFindArgs, conflictArgs);
                        ArrayList<String> reproductArgs = new ArrayList<>();

                        for (String cannotFindArg : cannotFindArgs) {
                            if (cannotFindArg.contains(" bet")) {
                                String[] indexInfos = queryAnalyzer.getArgsAndIndex().
                                        get(cannotFindArg).get(0).split(" ");
                                indexInfos[1] = indexInfos[1].replace("'", "");
                                indexInfos[3] = indexInfos[3].replace("'", "");
                                HashMap<String, List<String>> tempInfo = new HashMap<>();
                                tempInfo.put(cannotFindArg.split(" ")[0] + " >=", Collections.singletonList(indexInfos[1]));
                                ArrayList<String> tempList = new ArrayList<>();
                                sql = changeSql(sql, tempInfo, tempList, new ArrayList<>());
                                if (tempList.size() != 0) {
                                    tempInfo.clear();
                                    tempList.clear();
                                    tempInfo.put(cannotFindArg.split(" ")[0] + " >", Collections.singletonList(indexInfos[1]));
                                    sql = changeSql(sql, tempInfo, tempList, new ArrayList<>());
                                }
                                tempInfo.clear();
                                tempList.clear();
                                tempInfo.put(cannotFindArg.split(" ")[0] + " <=", Collections.singletonList(indexInfos[3]));
                                sql = changeSql(sql, tempInfo, tempList, new ArrayList<>());
                                if (tempList.size() != 0) {
                                    tempInfo.clear();
                                    tempList.clear();
                                    tempInfo.put(cannotFindArg.split(" ")[0] + " <", Collections.singletonList(indexInfos[3]));
                                    sql = changeSql(sql, tempInfo, tempList, new ArrayList<>());
                                }
                                reproductArgs.add(cannotFindArg);
                            }
                        }
                        cannotFindArgs.removeAll(reproductArgs);
                        if (cannotFindArgs.size() > 0) {

                            outputMessage += String.format("\033[31m  请注意%s中有参数无法完成替换，请查看该sql输出，手动替换;\033[0m", queryCanonicalName);
                            sqlWriter.write("-- cannotFindArgs:");
                            for (String cannotFindArg : cannotFindArgs) {
                                sqlWriter.write(cannotFindArg + ":" + queryAnalyzer.getArgsAndIndex().get(cannotFindArg) + ",");
                            }
                            sqlWriter.write("\n");
                        }
                        if (conflictArgs.size() > 0) {
                            outputMessage += String.format("\033[31m  请注意%s中有参数出现多次，无法智能，替换请查看该sql输出，手动替换;\033[0m", queryCanonicalName);
                            sqlWriter.write("-- conflictArgs:");
                            for (String conflictArg : conflictArgs) {
                                sqlWriter.write(conflictArg + ":" + queryAnalyzer.getArgsAndIndex().get(conflictArg) + ",");
                            }
                            sqlWriter.write("\n");
                        }
                        System.out.println(outputMessage);
                        sqlWriter.write(SQLUtils.format(sql, queryAnalyzer.getDbType(), SQLUtils.DEFAULT_LCASE_FORMAT_OPTION) + "\n");
                        sqlWriter.close();
                    } catch (TouchstoneToolChainException e) {
                        queryAnalyzer.outputSuccess(false);
                        System.out.println(String.format("%-15s Status:\033[31m获取失败\033[0m", queryCanonicalName));
                        System.out.println(e.getMessage());
                        for (String[] strings : queryPlan) {
                            for (String string : strings) {
                                System.out.print(string + "\t");
                            }
                            System.out.println();
                        }

                    }
                }
            }
        }
        System.out.println("获取查询计划完成");

        BufferedWriter schemaWriter = new BufferedWriter(new FileWriter(
                new File(writeDirectory.getPath() + "/schema.conf")));
        for (Schema schema : schemas.values()) {
            String schemaInfo = schema.formatSchemaInfo(), dataDistributionInfo = schema.formatDataDistributionInfo();
            if (schemaInfo != null) schemaWriter.write(schemaInfo + "\n");
            if (dataDistributionInfo != null) schemaWriter.write(dataDistributionInfo + "\n");
        }
        schemaWriter.close();
        BufferedWriter ccWriter = new BufferedWriter(new FileWriter(
                new File(writeDirectory.getPath() + "/constraintsChain.conf")));
        for (String queryInfo : queryInfos) {
            ccWriter.write(queryInfo + "\n");
        }
        ccWriter.close();
    }


}
