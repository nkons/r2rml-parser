package gr.seab.r2rml.entities;

import gr.seab.r2rml.entities.sql.SelectQuery;

import java.util.ArrayList;

/**
 * The subject map holds information about how to create the subjects of the mappings.
 * @see LogicalTableMapping
 *
 */
public class SubjectMap {

	private Template template;
	private SelectQuery selectQuery;
	private ArrayList<String> classUris;
	
	/**
	 * Default constructor.
	 */
	public SubjectMap() {
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
	 * @return the classUris
	 */
	public ArrayList<String> getClassUris() {
		return classUris;
	}
	/**
	 * @param classUris the classUris to set
	 */
	public void setClassUris(ArrayList<String> classUris) {
		this.classUris = classUris;
	}
}
