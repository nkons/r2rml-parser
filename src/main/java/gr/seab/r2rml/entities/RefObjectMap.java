package gr.seab.r2rml.entities;

public class RefObjectMap {

	private String parentTriplesMapUri;
	
	private String child;
	
	private String parent;

	public RefObjectMap() {
	}
	
	public String getParentTriplesMapUri() {
		return parentTriplesMapUri;
	}
	
	public void setParentTriplesMapUri(String parentTriplesMapUri) {
		this.parentTriplesMapUri = parentTriplesMapUri;
	}
	
	public String getChild() {
		return child;
	}

	public void setChild(String child) {
		this.child = child;
	}

	public String getParent() {
		return parent;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}
	
	
}
