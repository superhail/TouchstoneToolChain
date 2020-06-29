package ecnu.db.analyzer.online.node;

public class NodeTypeRefFactory {
    public static NodeTypeTool getNodeTypeRef(String tidbVersion) {
        if ("3.1.0".equals(tidbVersion)) {
            return new Tidb3NodeTypeTool();
        } else if ("4.0.0".equals(tidbVersion)) {
            return new Tidb4NodeTypeTool();
        }

        return null;
    }
}
