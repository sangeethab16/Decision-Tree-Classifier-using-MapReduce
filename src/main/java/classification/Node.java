package classification;

public class Node {

	private Integer id;
    private Integer leftChild;
    private Integer rightChild;
    private Integer attribute;
    private Float cutPoint;
    private String classLabel;
    
    public Node(String s) {
    	String[] nodeAttr = s.toString().split(",");
    	if (nodeAttr.length == 5) {
    		this.setId(Integer.parseInt(nodeAttr[0]));
    		this.setAttribute(Integer.parseInt(nodeAttr[1]));
    		this.setCutPoint(Float.parseFloat(nodeAttr[2]));
    		this.setLeftChild(Integer.parseInt(nodeAttr[3]));
    		this.setRightChild(Integer.parseInt(nodeAttr[4]));
    		this.setClassLabel("");
    		
    	}
    	else if (nodeAttr.length == 2) {
    		this.setId(Integer.parseInt(nodeAttr[0]));
    		this.setClassLabel(nodeAttr[1]);
    	}
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




	public String getClassLabel() {
		return classLabel;
	}




	public void setClassLabel(String classLabel) {
		this.classLabel = classLabel;
	}
	
    @Override
    public String toString() {
        return this.id + "," + this.leftChild + "," + this.rightChild + "," + this.classLabel;
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
    
}
