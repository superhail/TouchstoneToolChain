package ecnu.db.query.analyzer;


public class ExecutionNode {

    /**
     * 指向左节点
     */
    ExecutionNode rightNode;
    /**
     * 指向左节点
     */
    ExecutionNode leftNode;
    private final ExecutionNodeType type;
    private int outputRows;
    private String info;
    private boolean valueOutputted;
    private boolean pkOutputted;
    private int joinTag;

    public ExecutionNode(ExecutionNodeType type, int outputRows) {
        this.type = type;
        this.outputRows = outputRows;
        joinTag = -1;
    }

    public int getJoinTag() {
        return joinTag;
    }

    public void setJoinTag(int joinTag) {
        this.joinTag = joinTag;
    }

    public boolean isValueOutputted() {
        return valueOutputted;
    }

    public void setOutputted() {
        this.valueOutputted = true;
    }

    public boolean isPkOutputted() {
        return pkOutputted;
    }

    public void setPkOutputted() {
        this.pkOutputted = true;
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

    public int getOutputRows() {
        return outputRows;
    }

    public void setOutputRows(int outputRows) {
        this.outputRows = outputRows;
    }

    public ExecutionNodeType getType() {
        return type;
    }
}
