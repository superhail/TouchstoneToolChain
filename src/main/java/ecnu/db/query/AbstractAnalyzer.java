package ecnu.db.query;

import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.schema.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractAnalyzer {
    protected AbstractDbConnector dbConnector;
    private ArrayList<String> sqls;
    private HashMap<String, Schema> schemas;

    private final static Pattern MATH_OPERATION = Pattern.compile("<=|>=|=|<>|<|>|not like| like");
    private final static Pattern BINARY_OPERATION = Pattern.compile(" between");
    private final static Pattern MULTI_OPERATION = Pattern.compile("not in| in");


    AbstractAnalyzer(String file, AbstractDbConnector dbConnector) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        StringBuilder fileContents = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            if (!line.startsWith("--")) {
                line = line.trim();
                if (line.length() > 0) {
                    fileContents.append(line).append(' ');
                }
            }
        }
        sqls = new ArrayList<>();
        sqls.addAll(Arrays.asList(fileContents.toString().split(";")));
        this.dbConnector = dbConnector;
    }

    public void setSchemas(HashMap<String, Schema> schemas) {
        this.schemas = schemas;
    }

    public Queue<HashMap<String, TableQueryInfo>> staticAnalyzeSql(String sql) throws Exception {
        Queue<HashMap<String, TableQueryInfo>> queryConditions = new LinkedList<>();

        //解析select查询
        MySqlStatementParser mySqlStatementParser = new MySqlStatementParser(sql);
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) mySqlStatementParser.parseSelect();
        SQLSelect sqlSelect = sqlSelectStatement.getSelect();

        //获取sql查询块
        SQLSelectQueryBlock sqlSelectQueryBlock = (SQLSelectQueryBlock) sqlSelect.getQuery();
        Pair<ArrayList<String>, ArrayList<String>> tablesAndSubQueries =
                analyzeTableInfo(String.valueOf(sqlSelectQueryBlock.getFrom()));

        System.out.println(sqlSelectQueryBlock.getWhere());
        Pair<ArrayList<String>, ArrayList<String>> conditionsAndSubQueries =
                analyzeCondition(String.valueOf(sqlSelectQueryBlock.getWhere()));
        if (tablesAndSubQueries.getLeft().size() > 0) {
            queryConditions.add(analyzeTableAndConditions(tablesAndSubQueries.getLeft(), conditionsAndSubQueries.getLeft()));
            for (String subQuery : conditionsAndSubQueries.getRight()) {
                queryConditions.addAll(staticAnalyzeSql(subQuery));
            }
        }

        for (String subQuery : tablesAndSubQueries.getRight()) {
            queryConditions.addAll(staticAnalyzeSql(subQuery));
        }

        return queryConditions;
    }

    // todo 静态分析需要仔细考虑
    private HashMap<String, TableQueryInfo> analyzeTableAndConditions(ArrayList<String> tableNames, ArrayList<String> conditions) throws Exception {
        HashMap<String, TableQueryInfo> tableQueryInfos = new HashMap<>();
        for (String tableName : tableNames) {
            if (!schemas.containsKey(tableName)) {
                throw new Exception("没有解析出正确的tableName，解析结果为" + tableName);
            }
            tableQueryInfos.put(tableName, new TableQueryInfo(tableName));
        }

        for (String condition : conditions) {
            Matcher matcher;
            if ((matcher = MATH_OPERATION.matcher(condition)).find()) {
                String[] args = MATH_OPERATION.split(condition);
                if ("=".equals(matcher.group())) {
                    String leftTable;
                    String rightTable;
                    boolean leftIsKey = false;
                    for (String tableName : tableNames) {
                        Schema schema = schemas.get(tableName);
                        if(schema.isColumn(args[0])){
                            leftTable = tableName;
                            if(schema.isPrimaryKey(args[0])){

                            }
                        }
                        if(schema.isColumn(args[1])){
                            rightTable = tableName;
                            if(schema.isPrimaryKey(args[1])){

                            }
                        }
                    }
                } else {

                }
            } else if ((matcher = BINARY_OPERATION.matcher(condition)).find()) {
                System.out.println(matcher.group());
            } else if ((matcher = MULTI_OPERATION.matcher(condition)).find()) {
                System.out.println(matcher.group());
            }
        }

        return tableQueryInfos;
    }

    private Pair<ArrayList<String>, ArrayList<String>> analyzeTableInfo(String tableInfos) {
        //如果语句没有where条件语句，则返回null
        if (tableInfos == null || tableInfos.isBlank()) {
            return null;
        }

        // 格式化table信息，并做切分
        String[] tables = tableInfos.toLowerCase().replace('\n', ' ')
                .replace('\t', ' ').trim().replaceAll(" +", " ").split(" ");

        ArrayList<String> tableNames = new ArrayList<>();
        ArrayList<String> subQueries = new ArrayList<>();

        // 用于检测是否进入嵌套子查询的拼接
        int leftNum = 0;
        StringBuilder tempLine = new StringBuilder();
        for (String table : tables) {
            if (leftNum > 0) {
                // 拼接
                tempLine.append(table).append(" ");
                //如果遇到了(,则增加leftNum
                if (table.indexOf('(') != -1) {
                    leftNum++;
                }
                //如果遇到了),则减少leftNum
                if (table.indexOf(')') != -1) {
                    //如果减少到了0
                    if (--leftNum == 0) {
                        //记录嵌套子查询
                        subQueries.add(tempLine.substring(0, tempLine.lastIndexOf(")")));
                        //重新开始下一轮
                        tempLine = new StringBuilder(tempLine.substring(tempLine.lastIndexOf(")") + 1));
                    }
                }
            } else {
                if (table.indexOf(',') != -1) {
                    tableNames.add(tempLine.toString().trim());
                    tempLine = new StringBuilder();
                    table = table.substring(table.indexOf(',') + 1);
                }
                //如果没有遇到(,则正常拼接
                if (table.indexOf('(') == -1) {
                    tempLine.append(table).append(" ");
                } else {
                    tempLine = new StringBuilder(table.substring(table.indexOf('(') + 1) + " ");
                    leftNum = 1;
                }
            }
        }

        if (!tempLine.toString().isBlank()) {
            tableNames.add(tempLine.toString().trim());
        }

        return new ImmutablePair<>(tableNames, subQueries);
    }

    /**
     * 分析where条件语句，返回where中的条件和嵌套子查询。
     *
     * @param sqlConstraint where条件语句
     * @return where中的条件和嵌套子查询
     */
    private Pair<ArrayList<String>, ArrayList<String>> analyzeCondition(String sqlConstraint) {

        //如果语句没有where条件语句，则返回null
        if (sqlConstraint == null || sqlConstraint.isBlank()) {
            return null;
        }

        // 格式化where条件语句，并做切分
        String[] conditions = sqlConstraint.toLowerCase().replace('\n', ' ')
                .replace('\t', ' ').trim().replaceAll(" +", " ").split(" ");

        ArrayList<String> tableConditions = new ArrayList<>();
        ArrayList<String> subQueries = new ArrayList<>();


        StringBuilder tempLine = new StringBuilder();
        // 用于检测是否进入嵌套子查询的拼接
        int leftNum = 0;

        // 判断tempLine中是否存在between字段，存在时and字段不会被认定为分割
        boolean existBetween = false;

        for (int i = 0; i < conditions.length; i++) {
            String condition = conditions[i];
            // 如果在嵌套子查询拼接状态
            if (leftNum > 0) {
                // 拼接
                tempLine.append(condition).append(" ");

                //如果遇到了(,则增加leftNum
                if (condition.indexOf('(') != -1) {
                    leftNum++;
                }
                //如果遇到了),则减少leftNum
                if (condition.indexOf(')') != -1) {
                    //如果减少到了0
                    if (--leftNum == 0) {
                        //记录嵌套子查询
                        subQueries.add(tempLine.substring(0, tempLine.lastIndexOf(")")));
                        //重新开始下一轮
                        tempLine = new StringBuilder(tempLine.substring(tempLine.lastIndexOf(")") + 1));
                    }
                }
            } else {
                //如果遇到了and字段
                if ("and".equals(condition)) {
                    //如果之前有遇到between，则重置existBetween状态，并加入此语句，否则收集到了一个condition
                    if (existBetween) {
                        existBetween = false;
                        tempLine.append(" ").append(condition);
                    } else {
                        tableConditions.add(tempLine.toString());
                        tempLine = new StringBuilder();
                    }
                } else {
                    //如果没有遇到(,则正常拼接
                    if (condition.indexOf('(') == -1) {
                        //如果字段是between，则设置existBetween状态。
                        if ("between".equals(condition)) {
                            existBetween = true;
                        }
                        tempLine.append(condition).append(" ");
                    } else {
                        //如果遇到(,且包含select或者下一个字段是select，则清空之前遇到的字段，并设置leftNum，否则正常添加
                        if (condition.contains("select") || "select".equals(conditions[i + 1])) {
                            tempLine = new StringBuilder(condition.substring(condition.indexOf('(') + 1));
                            leftNum = 1;
                        } else {
                            tempLine.append(condition).append(" ");
                        }
                    }

                }
            }
        }
        if (!tempLine.toString().isBlank()) {
            tableConditions.add(tempLine.toString());
        }
        return new ImmutablePair<>(tableConditions, subQueries);
    }

    abstract String[] getSqlInfoColumns();

    abstract ExecutionNode getExecutionNodesRoot() throws Exception;

    public ArrayList<String> getSqls() {
        return sqls;
    }

    ArrayList<String[]> getQueryPlan() throws SQLException {
        return dbConnector.explainQuery(sqls.get(0), getSqlInfoColumns());
    }
}
