/**
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 Unported 
 * License (the "License"). You may not use this file except in compliance with
 * the License. You may obtain a copy of the License at:
 * 
 *  http://creativecommons.org/licenses/by-nc/4.0/
 *  
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
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
