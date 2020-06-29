package ecnu.db.analyzer.online.node;

public class NodeTypeRefFactory {
    public static NodeTypeTool getNodeTypeRef(String tidbVersion) {
        return new TidbNodeTypeTool();
    }
}
