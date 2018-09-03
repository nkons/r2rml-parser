package gr.seab.r2rml.entities.sparql;

import java.util.ArrayList;

/**
 * Holds information about a row in a SPARQL query resultset
 *
 */
public class LocalResultRow {

	private ArrayList<LocalResource> resources;
	
	/**
	 * 
	 */
	public LocalResultRow() {
	}
	
	/**
	 * @return the resources
	 */
	public ArrayList<LocalResource> getResources() {
		return resources;
	}
	/**
	 * @param resources the resources to set
	 */
	public void setResources(ArrayList<LocalResource> resources) {
		this.resources = resources;
	}
}
