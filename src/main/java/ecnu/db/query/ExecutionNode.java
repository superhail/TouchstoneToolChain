package ecnu.db.query;


public class ExecutionNode {

    ExecutionNodeType type;
    int outputRows;
    String info;
    /**
     * 指向左节点
     */
    ExecutionNode rightNode;
    /**
     * 指向左节点
     */
    ExecutionNode leftNode;

    public ExecutionNode(ExecutionNodeType type, int outputRows) {
        this.type = type;
        this.outputRows = outputRows;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public ExecutionNode getRightNode() {
        return rightNode;
    }

    public void setRightNode(ExecutionNode rightNode) {
        this.rightNode = rightNode;
    }

    public ExecutionNode getLeftNode() {
        return leftNode;
    }

    public void setLeftNode(ExecutionNode leftNode) {
        this.leftNode = leftNode;
    }

    public ExecutionNodeType getType() {
        return type;
    }
}
