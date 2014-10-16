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
package gr.seab.r2rml.entities.sparql;

import java.io.Serializable;

/**
 * Holds information about a Sparql query and the resultset it returns
 * @see LocalResultSet
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
