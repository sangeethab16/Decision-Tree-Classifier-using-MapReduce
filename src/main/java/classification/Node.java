package classification;

public class Node {

	private Integer id;
	private Integer leftChild;
	private Integer rightChild;
	private Integer attribute;
	private Float cutPoint;
	private String leftFlag;
	private String rightFlag;

	public Node(String s) {
		
		String[] nodeAttr = s.toString().split(",");
		this.setId(Integer.parseInt(nodeAttr[0]));
		this.setAttribute(Integer.parseInt(nodeAttr[1]));
		this.setCutPoint(Float.parseFloat(nodeAttr[2]));
		this.setLeftChild(Integer.parseInt(nodeAttr[4]));
		this.setLeftFlag(nodeAttr[3]);
		this.setRightChild(Integer.parseInt(nodeAttr[6]));
		this.setRightFlag(nodeAttr[5]);

	}

	public Node() {
		leftFlag = "false";
		rightFlag = "false";
	}

	public Integer getLeftChild() {
		return leftChild;
	}

	public void setLeftChild(Integer leftChild) {
		this.leftChild = leftChild;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public Integer getRightChild() {
		return rightChild;
	}

	public void setRightChild(Integer rightChild) {
		this.rightChild = rightChild;
	}

	public Integer getAttribute() {
		return attribute;
	}

	public void setAttribute(Integer attribute) {
		this.attribute = attribute;
	}

	public Float getCutPoint() {
		return cutPoint;
	}

	public void setCutPoint(float f) {
		this.cutPoint = f;
	}

	public String getLeftFlag() {
		return leftFlag;
	}

	public void setLeftFlag(String leftFlag) {
		this.leftFlag = leftFlag;
	}

	public String getRightFlag() {
		return rightFlag;
	}

	public void setRightFlag(String rightFlag) {
		this.rightFlag = rightFlag;
	}

}
