/**
 * 
 */
package gr.ekt.r2rml.entities;

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
	 * The language of the literals to be created.
	 */
	private Template language;
	
	/**
	 * One of the XSD datatypes.
	 */
	private BaseDatatype dataType;
	
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
	
	public Template getLanguage() {
		return language;
	}
	
	public void setLanguage(Template language) {
		this.language = language;
	}
	
	public BaseDatatype getDataType() {
		return dataType;
	}
	
	public void setDataType(BaseDatatype dataType) {
		this.dataType = dataType;
	}
	
}
