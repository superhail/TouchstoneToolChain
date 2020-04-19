package ecnu.db.analyzer.online;

import com.alibaba.druid.util.JdbcConstants;
import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.analyzer.online.ExecutionNode.ExecutionNodeType;
import ecnu.db.schema.Schema;
import ecnu.db.utils.TouchstoneToolChainException;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TidbAnalyzer extends AbstractAnalyzer {
    private static final Pattern ROW_COUNTS = Pattern.compile("rows:[0-9]*");
    private static final Pattern INNER_JOIN_OUTER_KEY = Pattern.compile("outer key:.*,");
    private static final Pattern INNER_JOIN_INNER_KEY = Pattern.compile("inner key:.*");
    private static final Pattern JOIN_EQ_OPERATOR = Pattern.compile("equal:\\[.*]");
    HashSet<String> readerType = new HashSet<>(Arrays.asList("TableReader", "IndexReader", "IndexLookUp"));
    HashSet<String> passNodeType = new HashSet<>(Arrays.asList("Projection", "TopN", "Sort", "HashAgg", "StreamAgg", "TableScan", "IndexScan"));
    HashSet<String> joinNodeType = new HashSet<>(Arrays.asList("HashRightJoin", "HashLeftJoin", "IndexMergeJoin", "IndexHashJoin", "IndexJoin"));
    HashSet<String> filterNodeType = new HashSet<>(Collections.singletonList("Selection"));
    HashMap<String, String> tidbSelectArgs;


    public TidbAnalyzer(AbstractDbConnector dbConnector, HashMap<String, String> tidbSelectArgs,
                        HashMap<String, Schema> schemas) {
        super(dbConnector, schemas);
        this.tidbSelectArgs = tidbSelectArgs;
    }

    @Override
    String[] getSqlInfoColumns() {
        return new String[]{"id", "operator info", "execution info"};
    }

    @Override
    public String getDbType() {
        return JdbcConstants.MYSQL;
    }

    @Override
    public ExecutionNode getExecutionTree(List<String[]> queryPlan) throws TouchstoneToolChainException {
        //清空参数和位置的对应关系
        argsAndIndex.clear();

        Stack<ExecutionNode> nodes = new Stack<>();
        // 查询树上一层级的位置和当前的位置，用于判断是否出现了层级变动，方便构建查询树
        int lastLevel = -1, currentLevel = 0;

        // 解析出的节点的类别
        String nodeType;
        Matcher m;
        for (int i = 0; i < queryPlan.size(); i++) {
            String[] subQueryPlan = queryPlan.get(i);
            // 分析查询节点的位置
            String[] levelAndType = subQueryPlan[0].split("─");
            //如果等于1，则为初始节点
            if (levelAndType.length == 1) {
                nodeType = levelAndType[0].split("_")[0];
            } else {
                //通过"-"前的字符判定层级
                currentLevel = (levelAndType[0].length() - 1) / 2;
                nodeType = levelAndType[1].split("_")[0];
            }

            // 如果不分析此类型，则继续扫描
            if (!passNodeType.contains(nodeType)) {
                ExecutionNode executionNode;

                // 抽取 rowCount，失败时rowCount等于0
                int rowCount = (m = ROW_COUNTS.matcher(subQueryPlan[2])).find() ?
                        Integer.parseInt(m.group(0).split(":")[1]) : 0;
                // 如果属于join，则记录信息
                if (joinNodeType.contains(nodeType)) {
                    executionNode = new ExecutionNode(ExecutionNodeType.join, rowCount, subQueryPlan[1]);
                }
                // 如果过滤节点，记录信息
                else if (filterNodeType.contains(nodeType)) {
                    executionNode = new ExecutionNode(ExecutionNodeType.filter, rowCount, subQueryPlan[1]);
                    //跳过下层的table scan
                    i++;
                } else if (readerType.contains(nodeType)) {
                    //在树中维护下一层的信息
                    i++;
                    //判断下一层是不是selection
                    boolean isSelection = false;
                    for (String filterKeyWord : filterNodeType) {
                        if (queryPlan.get(i)[0].contains(filterKeyWord)) {
                            isSelection = true;
                            break;
                        }
                    }
                    //如果下一层是selection, 则跳过下层的table scan操作
                    if (isSelection) {
                        executionNode = new ExecutionNode(ExecutionNodeType.filter, queryPlan.get(i++)[1]);
                    } else {
                        executionNode = new ExecutionNode(ExecutionNodeType.scan, queryPlan.get(i)[1]);
                    }
                }
                // 如果在三类节点中都没有找到匹配，则需要报错
                else {
                    throw new TouchstoneToolChainException("未支持的查询树Node，类型为" + nodeType);
                }

                // 计算查询树的层级
                if (currentLevel > lastLevel) {
                    if (!nodes.empty()) {
                        nodes.peek().setLeftNode(executionNode);
                    }
                } else {
                    try {
                        while (nodes.peek().getType() != ExecutionNodeType.join || nodes.peek().getRightNode() != null) {
                            nodes.pop();
                        }
                    } catch (EmptyStackException e) {
                        e.printStackTrace();
                    }
                    nodes.peek().setRightNode(executionNode);
                }
                nodes.push(executionNode);
                lastLevel = currentLevel;
            }
        }

        return nodes.get(0);
    }


    /**
     * 分析join信息
     *
     * @param joinInfo join字符串
     * @return 长度为4的字符串数组，0，1为join info左侧的表名和列名，2，3为join右侧的表明和列名
     * @throws TouchstoneToolChainException 无法分析的join条件
     */
    @Override
    public String[] analyzeJoinInfo(String joinInfo) throws TouchstoneToolChainException {
        if (joinInfo.contains("other cond:")) {
            throw new TouchstoneToolChainException("join中包含其他条件,暂不支持");
        }
        String[] result = new String[4];
        Matcher eqCondition = JOIN_EQ_OPERATOR.matcher(joinInfo);
        if (eqCondition.find()) {
            if (eqCondition.groupCount() > 1) {
                throw new UnsupportedOperationException();
            }
            String[] eqInfos = eqCondition.group(0).substring(0, eqCondition.group(0).length() - 1).split("eq\\(");
            boolean multiTables = false;
            for (int i = 1; i < eqInfos.length; i++) {
                String eqInfo = eqInfos[i];
                String[] joinInfos = eqInfo.split(",");
                String[] leftJoinInfos = joinInfos[0].split("\\.");
                String[] rightJoinInfos = joinInfos[1].split("\\.");
                if (!multiTables) {
                    result[0] = leftJoinInfos[1];
                    result[1] = leftJoinInfos[2];
                    result[2] = rightJoinInfos[1];
                    result[3] = rightJoinInfos[2];
                } else {
                    if (!result[0].equals(leftJoinInfos[1]) || !result[2].equals(rightJoinInfos[1])) {
                        throw new TouchstoneToolChainException("join中包含多个表的约束,暂不支持");
                    }
                    result[1] += "," + leftJoinInfos[2];
                    result[3] += "," + rightJoinInfos[2];
                }
                multiTables = true;
            }
        } else {
            Matcher innerInfo = INNER_JOIN_INNER_KEY.matcher(joinInfo);
            if (innerInfo.find()) {
                String[] innerInfos = innerInfo.group(0).split("\\.");
                result[0] = innerInfos[1];
                result[1] = innerInfos[2];
            } else {
                throw new TouchstoneToolChainException("无法匹配的join格式" + joinInfo);
            }
            Matcher outerInfo = INNER_JOIN_OUTER_KEY.matcher(joinInfo);
            if (outerInfo.find()) {
                String[] outerInfos = outerInfo.group(0).split("\\.");
                result[2] = outerInfos[1];
                result[3] = outerInfos[2].substring(0, outerInfos[2].length() - 1);
            } else {
                throw new TouchstoneToolChainException("无法匹配的join格式" + joinInfo);
            }
        }
        if (result[1].contains(")")) {
            result[1] = result[1].substring(0, result[1].indexOf(')'));
        }
        if (result[3].contains(")")) {
            result[3] = result[3].substring(0, result[3].indexOf(')'));
        }
        return convertToDbTableName(result);
    }

    private String[] convertToDbTableName(String[] result) {
        if (aliasDic != null && aliasDic.containsKey(result[0])) {
            result[0] = aliasDic.get(result[0]);
        }
        if (aliasDic != null && aliasDic.containsKey(result[2])) {
            result[2] = aliasDic.get(result[2]);
        }
        return result;
    }


    /**
     * 分析传入的select 过滤条件，传出表名和格式化后的condition
     * todo 增加对or的支持
     *
     * @param selectCondition 传入的select条件语句
     * @return 表名和格式化后的condition
     */
    @Override
    Pair<String, String> analyzeSelectCondition(String selectCondition) throws TouchstoneToolChainException {
        String tableName = null;
        HashMap<String, String> conditionString = new HashMap<>();
        boolean useAlias = false;
        String[] conditions = selectCondition.split("\\),");
        int inCount = -1;
        for (String condition : conditions) {
            String[] conditionInfos = condition.split("\\(");
            String operator;
            String columnName;
            if ("not".equals(conditionInfos[0].trim())) {
                operator = "not " + convertOperator(conditionInfos[1]);
                String[] operatorInfo = conditionInfos[2].split(",");
                columnName = operatorInfo[0].split("\\.")[2];
                if ("not isnull".equals(operator)) {
                    while (columnName.endsWith(")")) {
                        columnName = columnName.substring(0, columnName.length() - 1);
                    }
                }
                if (tableName == null) {
                    tableName = operatorInfo[0].split("\\.")[1];
                    if (aliasDic != null && aliasDic.containsKey(tableName)) {
                        useAlias = true;
                    }
                } else {
                    if (!tableName.equals(operatorInfo[0].split("\\.")[1])) {
                        throw new TouchstoneToolChainException("select的表名不一致");
                    }
                }

                if ("not in".equals(operator)) {
                    operator += "(" + (operatorInfo.length - 1) + ")";
                    inCount = operatorInfo.length - 1;
                }
            } else {
                operator = convertOperator(conditionInfos[0]);
                String[] operatorInfo = conditionInfos[1].split(",");
                columnName = operatorInfo[0].split("\\.")[2];
                if ("isnull".equals(operator)) {
                    while (columnName.endsWith(")")) {
                        columnName = columnName.substring(0, columnName.length() - 1);
                    }
                }
                if (tableName == null) {
                    tableName = operatorInfo[0].split("\\.")[1];
                    if (aliasDic != null && aliasDic.containsKey(tableName)) {
                        useAlias = true;
                    }
                } else {
                    if (!tableName.equals(operatorInfo[0].split("\\.")[1])) {
                        throw new TouchstoneToolChainException("select的表名不一致");
                    }
                }
                if ("in".equals(operator)) {
                    operator += "(" + (operatorInfo.length - 1) + ")";
                    inCount = operatorInfo.length - 1;
                }
            }
            if (conditionString.containsKey(columnName)) {
                conditionString.put(columnName, "bet");
            } else {
                conditionString.put(columnName, operator);
            }
        }
        if (aliasDic != null && aliasDic.containsKey(tableName)) {
            tableName = aliasDic.get(tableName);
        }
        StringBuilder conditionFinalString = new StringBuilder();
        for (Map.Entry<String, String> stringStringEntry : conditionString.entrySet()) {
            conditionFinalString.append(stringStringEntry.getKey()).append("@").append(stringStringEntry.getValue()).append("#");
            String columnName = stringStringEntry.getKey();
            String operator = stringStringEntry.getValue();
            String selectArgName = columnName + " " + operator;
            if (useAlias) {
                selectArgName = tableName + "." + selectArgName;
            }
            StringBuilder selectArgs = new StringBuilder();

            if (operator.contains("in")) {
                int dateOrNot = schemas.get(tableName).isDate(columnName) ? 1 : 0;
                for (int i = 0; i < inCount; i++) {
                    selectArgs.append("'#").append(sqlArgIndex).append(",").append(i).append(",").append(dateOrNot).append("#', ");
                }
                sqlArgIndex++;
                selectArgs = new StringBuilder("(" + selectArgs.substring(0, selectArgs.length() - 2) + ")");
            } else {
                if ("bet".equals(operator)) {
                    selectArgs.append("ween '#").append(sqlArgIndex).append(",0,").append(schemas.get(tableName)
                            .isDate(columnName) ? 1 : 0).append("#' and '#").append(sqlArgIndex++).append(",1,")
                            .append(schemas.get(tableName).isDate(columnName) ? 1 : 0).append("#'");
                } else {
                    selectArgs.append("#").append(sqlArgIndex++).append(",0,").append(schemas.get(tableName)
                            .isDate(columnName) ? 1 : 0).append("#");
                }
            }
            if (argsAndIndex.containsKey(selectArgName)) {
                argsAndIndex.get(selectArgName).add(selectArgs.toString());
            } else {
                List<String> value = new ArrayList<>();
                value.add(selectArgs.toString());
                argsAndIndex.put(selectArgName, value);
            }
        }
        if (conditionString.size() > 1) {
            conditionFinalString.append("and");
        }


        return new MutablePair<>(tableName, conditionFinalString.toString());
    }

    public String convertOperator(String tidbOperator) throws TouchstoneToolChainException {
        tidbOperator = tidbOperator.trim().toUpperCase();
        if (tidbSelectArgs.containsKey(tidbOperator)) {
            return tidbSelectArgs.get(tidbOperator);
        } else {
            throw new TouchstoneToolChainException("没有指定的operator转换：" + tidbOperator);
        }
    }
}
