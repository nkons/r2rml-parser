/**
 * 
 */
package gr.ekt.r2rml.entities.sparql;


import java.io.Serializable;

/**
 * @author nkons
 *
 */
public class SparqlQuery implements Serializable {

	private static final long serialVersionUID = 7934008387554302L;
	
	private String query;
	private LocalResultSet resultSet;

	/**
	 * 
	 */
	public SparqlQuery() {
	}
	
	
	/**
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the resultSet
	 */
	public LocalResultSet getResultSet() {
		return resultSet;
	}

	/**
	 * @param resultSet the resultSet to set
	 */
	public void setResultSet(LocalResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
}
