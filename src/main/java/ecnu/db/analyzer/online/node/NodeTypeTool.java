package ecnu.db.analyzer.online.node;


/**
 * @author lianxuechao
 */
public interface NodeTypeTool {
    boolean isReaderNode(String nodeType);

    boolean isPassNode(String nodeType);

    boolean isJoinNode(String nodeType);

    boolean isFilterNode(String nodeType);

    boolean isTableScanNode(String nodeType);

    boolean isIndexScanNode(String nodeType);
}
