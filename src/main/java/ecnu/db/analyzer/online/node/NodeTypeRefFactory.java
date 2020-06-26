package ecnu.db.analyzer.online.node;

public class NodeTypeRefFactory {
    public static NodeTypeRef getNodeTypeRef(String tidbVersion) {
        if ("3.1.0".equals(tidbVersion)) {
            return new Tidb3NodeTypeRef();
        } else if ("4.0.0".equals(tidbVersion)) {
            return new Tidb4NodeTypeRef();
        }

        return null;
    }
}
