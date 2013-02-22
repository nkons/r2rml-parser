/**
 * 
 */
package gr.ekt.r2rml.entities;

import gr.ekt.r2rml.entities.sql.SelectQuery;

/**
 * The logical view of the mapping can be either the fields of a table or an arbitrary sql query.
 * @author nkons
 *
 */
public class LogicalTableView {

	/**
	 * The uri of this view. It has a value for declared sql queries and is null when the mapping is on a table
	 */
	private String uri;
	private SelectQuery query;
	
	/**
	 * Default no-argument constructor
	 */
	public LogicalTableView() {
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
	 * @return the query
	 */
	public SelectQuery getQuery() {
		return query;
	}
	/**
	 * @param query the query to set
	 */
	public void setQuery(SelectQuery query) {
		this.query = query;
	}
	
	
}
