/**
 * 
 */
package gr.ekt.r2rml.components;

/**
 * The mapping of predicates and respective objects for a specific subject. Objects come either from a column or from a template.
 * @author nkons
 *
 */
public class PredicateObjectMap {

	/**
	 * The predicate value.
	 */
	private String predicate;
	
	/**
	 * Holds the template through which the objects will be generated.
	 */
	private Template objectTemplate;
	
	
	/**
	 * Holds the value of an rr:column definition. It is null in the case of templates.
	 */
	private String objectColumn;
	
	/**
	 * 
	 */
	public PredicateObjectMap() {
	}
	
	/**
	 * @return the predicate
	 */
	public String getPredicate() {
		return predicate;
	}
	/**
	 * @param predicate the predicate to set
	 */
	public void setPredicate(String predicate) {
		this.predicate = predicate;
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
}
