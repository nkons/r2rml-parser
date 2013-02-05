/**
 * 
 */
package gr.ekt.r2rml.components;

import java.util.ArrayList;

/**
 * The subject map holds information about how to create the subjects of the mappings.
 * @see LogicalTableMapping
 * @author nkons
 *
 */
public class SubjectMap {

	private ArrayList<String> fields;
	private Template template;
	private SelectQuery selectQuery;
	private String classUri;
	
	/**
	 * Default constructor.
	 */
	public SubjectMap() {
	}
	
	/**
	 * @return the fields
	 */
	public ArrayList<String> getFields() {
		return fields;
	}
	/**
	 * @param fields the fields to set
	 */
	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}
	/**
	 * @return the template
	 */
	public Template getTemplate() {
		return template;
	}
	/**
	 * @param template the template to set
	 */
	public void setTemplate(Template template) {
		this.template = template;
	}
	/**
	 * @return the selectQuery
	 */
	public SelectQuery getSelectQuery() {
		return selectQuery;
	}
	/**
	 * @param selectQuery the selectQuery to set
	 */
	public void setSelectQuery(SelectQuery selectQuery) {
		this.selectQuery = selectQuery;
	}
	/**
	 * @return the classUri
	 */
	public String getClassUri() {
		return classUri;
	}
	/**
	 * @param classUri the classUri to set
	 */
	public void setClassUri(String classUri) {
		this.classUri = classUri;
	}
}
