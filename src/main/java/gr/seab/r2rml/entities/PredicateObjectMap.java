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

import java.util.ArrayList;

import com.hp.hpl.jena.datatypes.BaseDatatype;

/**
 * The mapping of predicates and respective objects for a specific subject. Objects come either from a column or from a template.
 * @author nkons
 *
 */
public class PredicateObjectMap {

	/**
	 * The predicate value(s).
	 */
	private ArrayList<String> predicates;
	
	/**
	 * Holds the template through which the objects will be generated.
	 */
	private Template objectTemplate;
	
	/**
	 * Holds the value of an rr:column definition. It is null in the case of templates.
	 */
	private String objectColumn;
	
	/**
	 * One of the XSD datatypes.
	 */
	private BaseDatatype dataType;

	/**
	 * If the predicateObjectMap is a rr:RefObjectMap, i.e. references a rr:parentTriplesMap, store the referenced uri, child and parent fields in an object.
	 */
	private RefObjectMap refObjectMap;
	
	/**
	 * 
	 */
	public PredicateObjectMap() {
	}
	
	/**
	 * @return the predicates
	 */
	public ArrayList<String> getPredicates() {
		return predicates;
	}
	/**
	 * @param predicates the predicates to set
	 */
	public void setPredicates(ArrayList<String> predicates) {
		this.predicates = predicates;
	}
	/**
	 * @return the objectTemplate
	 */
	public Template getObjectTemplate() {
		return objectTemplate;
	}
	/**
	 * @param objectTemplate the objectTemplate to set
	 */
	public void setObjectTemplate(Template objectTemplate) {
		this.objectTemplate = objectTemplate;
	}
	/**
	 * @return the objectColumn
	 */
	public String getObjectColumn() {
		return objectColumn;
	}
	/**
	 * @param objectColumn the objectColumn to set
	 */
	public void setObjectColumn(String objectColumn) {
		this.objectColumn = objectColumn;
	}
	
	public BaseDatatype getDataType() {
		return dataType;
	}
	
	public void setDataType(BaseDatatype dataType) {
		this.dataType = dataType;
	}
	
	public RefObjectMap getRefObjectMap() {
		return refObjectMap;
	}
	
	public void setRefObjectMap(RefObjectMap refObjectMap) {
		this.refObjectMap = refObjectMap;
	}
	
}
