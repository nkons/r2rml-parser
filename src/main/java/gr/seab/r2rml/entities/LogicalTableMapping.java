/**
 * Licensed under the Creative Commons Attribution-NonCommercial 3.0 Unported 
 * License (the "License"). You may not use this file except in compliance with
 * the License. You may obtain a copy of the License at:
 * 
 *  http://creativecommons.org/licenses/by-nc/3.0/
 *  
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gr.seab.r2rml.entities;

import java.util.ArrayList;

import com.hp.hpl.jena.rdf.model.Statement;

/**
 * Logical table mappings can either map an arbitrary query or a single table name
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
	 * Hold the resulting subjects, needed when referenced by a parentTriplesMap
	 */
	private ArrayList<String> subjects;
	
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
	
	public ArrayList<String> getSubjects() {
		return subjects;
	}
	
	public void setSubjects(ArrayList<String> subjects) {
		this.subjects = subjects;
	}
}
