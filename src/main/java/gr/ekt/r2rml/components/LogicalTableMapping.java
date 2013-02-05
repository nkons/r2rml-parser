/**
 * 
 */
package gr.ekt.r2rml.components;

import java.util.ArrayList;

/**
 * Logical table mappings can either map an arbitrary query or a single table name.
 * @author nkons
 *
 */
public class LogicalTableMapping {

	/**
	 * The uri of the mapping in the mapping document
	 */
	private String uri;
	
	
	/**
	 * The view that is associated with this mapping 
	 */
	private LogicalTableView view;
	
	/**
	 * The subject mapping 
	 */
	private SubjectMap subjectMap = new SubjectMap();
	
	/**
	 * The list of the mappings that hold the respective predicate and object values for this mapping's subject
	 */
	private ArrayList<PredicateObjectMap> predicateObjectMaps = new ArrayList<PredicateObjectMap>();
	
	/**
	 * Default no-argument constructor
	 */
	public LogicalTableMapping() {
	}
	
	/**
	 * @return the subjectMap
	 */
	public SubjectMap getSubjectMap() {
		return subjectMap;
	}
	/**
	 * @param subjectMap the subjectMap to set
	 */
	public void setSubjectMap(SubjectMap subjectMap) {
		this.subjectMap = subjectMap;
	}
	
	/**
	 * @return the predicateObjectMaps
	 */
	public ArrayList<PredicateObjectMap> getPredicateObjectMaps() {
		return predicateObjectMaps;
	}
	/**
	 * @param predicateObjectMaps the predicateObjectMaps to set
	 */
	public void setPredicateObjectMaps(
			ArrayList<PredicateObjectMap> predicateObjectMaps) {
		this.predicateObjectMaps = predicateObjectMaps;
	}
	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}
	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}
	/**
	 * @return the view
	 */
	public LogicalTableView getView() {
		return view;
	}
	/**
	 * @param view the view to set
	 */
	public void setView(LogicalTableView view) {
		this.view = view;
	}
}
