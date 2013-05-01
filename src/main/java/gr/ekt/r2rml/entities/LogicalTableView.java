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
	
	/**
	 * The SQL Select query that is associated with this view
	 */
	private SelectQuery selectQuery;
	
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
	 * @return the selectQuery
	 */
	public SelectQuery getSelectQuery() {
		return selectQuery;
	}
	/**
	 * @param query the selectQuery to set
	 */
	public void setSelectQuery(SelectQuery selectQuery) {
		this.selectQuery = selectQuery;
	}
	
	
}
