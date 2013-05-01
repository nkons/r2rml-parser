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
package gr.ekt.r2rml.entities.sparql;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds information about a Sparql query results
 * @see LocalResultRow
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
