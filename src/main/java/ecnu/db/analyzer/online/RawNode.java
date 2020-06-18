package ecnu.db.analyzer.online;

import java.util.Arrays;

public class RawNode {
    public RawNode left;
    public RawNode right;
    public String nodeType;
    public String[] data;

    public RawNode(RawNode left, RawNode right, String nodeType, String[] data) {
        this.left = left;
        this.right = right;
        this.nodeType = nodeType;
        this.data = data;
    }

    @Override
    public String toString() {
        return "RawNode{data=" + Arrays.toString(data) + "}";
    }
}
