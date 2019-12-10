package ecnu.db.query;

import ecnu.db.dbconnector.AbstractDbConnector;
import ecnu.db.dbconnector.TidbConnector;
import ecnu.db.utils.SystemConfig;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Stack;

public class TidbAnalyzer extends AbstractAnalyzer {
    HashSet<String> passNodeType = new HashSet<>(Arrays.asList("Projection", "TopN", "TableReader", "Sort", "HashAgg", "IndexReader", "StreamAgg"));
    HashSet<String> joinNodeType = new HashSet<>(Arrays.asList("HashRightJoin", "IndexMergeJoin", "IndexHashJoin", "HashLeftJoin", "IndexJoin", "IndexLookUp"));
    HashSet<String> filterNodeType = new HashSet<>(Arrays.asList("Selection", "TableScan", "IndexScan"));

    public TidbAnalyzer(String file, AbstractDbConnector dbConnector) throws IOException {
        super(file, dbConnector);
    }

    public static String getTable(ExecutionNode node, int level) {
        if (node != null) {
            if (node.getInfo() != null) {
                return node.getInfo();
            }
            return getTable(node.getLeftNode(), level + 1) + "," + getTable(node.getRightNode(), level + 1);
        }
        return "";
    }

    @Override
    String[] getSqlInfoColumns() {
        return new String[]{"id", "operator info", "execution info"};
    }

    @Override
    ExecutionNode getExecutionNodesRoot() throws Exception {
        Stack<ExecutionNode> nodes = new Stack<>();
        int currentLevel = 0;
        try {
            ArrayList<String[]> ss = getQueryPlan();
            for (String[] s : ss) {
                String[] levelAndType = s[0].split("─");
                int level;
                String nodeType;
                if (levelAndType.length > 1) {
                    level = (levelAndType[0].length() - 1) / 2;
                    nodeType = levelAndType[1].split("_")[0];
                } else {
                    level = 0;
                    nodeType = levelAndType[0].split("_")[0];
                }
                ExecutionNode executionNode;
                if (!passNodeType.contains(nodeType)) {
                    if (joinNodeType.contains(nodeType)) {
                        executionNode = new ExecutionNode(ExecutionNodeType.join, 0);
                    } else if (filterNodeType.contains(nodeType)) {
                        executionNode = new ExecutionNode(ExecutionNodeType.filter, 0);
                        if ("TableScan".equals(nodeType) || "IndexScan".equals(nodeType)) {
                            executionNode.setInfo(s[1].split(",")[0]);
                        }
                    } else {
                        throw new Exception("未支持的查询树Node，类型为" + nodeType);
                    }
//                    executionNode.setInfo(levelAndType[1]);
                    if (level > currentLevel) {
                        if (!nodes.empty()) {
                            nodes.peek().setLeftNode(executionNode);
                        }
                        nodes.push(executionNode);
                    } else {
                        while (nodes.peek().getType() != ExecutionNodeType.join || nodes.peek().getRightNode() != null) {
                            nodes.pop();
                        }
                        nodes.peek().setRightNode(executionNode);
                        nodes.push(executionNode);
                    }
                    currentLevel = level;
                }
            }

            ExecutionNode root = null;
            while (!nodes.empty()) {
                root = nodes.pop();
            }
            outputNode(root, 0);
            return root;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void outputNode(ExecutionNode node, int level) {
        if (node != null) {
            if (node.getInfo() != null) {
                System.out.println(" ".repeat(Math.max(0, level)) + node.getInfo());
            }
            outputNode(node.getLeftNode(), level + 1);
            outputNode(node.getRightNode(), level + 1);
        }
    }
}
