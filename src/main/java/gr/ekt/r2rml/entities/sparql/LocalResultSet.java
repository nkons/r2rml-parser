/**
 * 
 */
package gr.ekt.r2rml.entities.sparql;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information about a SPARQL query results
 * @author nkons
 *
 */
public class LocalResultSet {

	private List<String> variables;
	private ArrayList<LocalResultRow> rows;
	
	/**
	 * 
	 */
	public LocalResultSet() {
	}
	
	/**
	 * @return the variables
	 */
	public List<String> getVariables() {
		return variables;
	}
	/**
	 * @param variables the variables to set
	 */
	public void setVariables(List<String> variables) {
		this.variables = variables;
	}
	/**
	 * @return the rows
	 */
	public ArrayList<LocalResultRow> getRows() {
		return rows;
	}
	/**
	 * @param rows the rows to set
	 */
	public void setRows(ArrayList<LocalResultRow> rows) {
		this.rows = rows;
	}
	
}
