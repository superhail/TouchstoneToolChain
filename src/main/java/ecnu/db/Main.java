package ecnu.db;


import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.query.AbstractAnalyzer;
import ecnu.db.query.TidbAnalyzer;
import ecnu.db.schema.Schema;
import ecnu.db.schema.generation.AbstractSchemaGeneration;
import ecnu.db.schema.generation.TidbSchemaGeneration;
import ecnu.db.utils.SystemConfig;

import java.util.ArrayList;
import java.util.HashMap;

public class Main {

    public static void main(String[] args) throws Exception {
        SystemConfig systemConfig = SystemConfig.readConfig("tool.json");
        AbstractDbConnector dbConnector = new TidbConnector(systemConfig);
        AbstractSchemaGeneration dbSchemaGeneration = new TidbSchemaGeneration();
        HashMap<String, Schema> schemas = makeSchema(dbConnector, dbSchemaGeneration);

        for (int i = 1; i <= 16; i++) {
            if (i == 13||i==15) {
                continue;
            }
            try {
                System.out.println("query" + i + "____________________________________");
                AbstractAnalyzer abstractAnalyzer = new TidbAnalyzer("sqls/" + i + ".sql", dbConnector);
                abstractAnalyzer.setSchemas(schemas);
                abstractAnalyzer.staticAnalyzeSql(abstractAnalyzer.getSqls().get(0));
//                ExecutionNode root = abstractAnalyzer.getExecutionNodesRoot();
//                System.out.println(getTable(root, 0));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private static HashMap<String, Schema> makeSchema(AbstractDbConnector dbConnector, AbstractSchemaGeneration dbSchemaGeneration) throws Exception {
        ArrayList<String> tableNames = dbConnector.getTableNames();
        HashMap<String, Schema> schemas = new HashMap<>();
        for (String tableName : tableNames) {
            Schema schema = dbSchemaGeneration.generateSchema(tableName, dbConnector.getCreateTableSql(tableName));
//            dbSchemaGeneration.setDataRangeBySqlResult(schema.getNotKeyColumns(), dbConnector.getDataRange(tableName,
//                    dbSchemaGeneration.getColumnDistributionSql(schema.getNotKeyColumns())));
//            dbSchemaGeneration.setDataRangeUnique(schema, dbConnector);
            schemas.put(tableName, schema);
        }
//        for (Schema schema : schemas) {
//            System.out.println(schema.formatSchemaInfo());
//            System.out.println(schema.formatDataDistributionInfo());
//        }
        return schemas;
    }
}
