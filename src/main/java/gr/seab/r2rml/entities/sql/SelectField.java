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
package gr.seab.r2rml.entities.sql;


/**
 * A select field in an sql select query
 * @see SelectQuery
 * @author nkons
 *
 */
public class SelectField {

	private String name;
	private String alias;
	
	private SelectTable table;
	
	/**
	 * Default no-argument constructor.
	 */
	public SelectField() {
	}
	
	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}
	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the alias
	 */
	public String getAlias() {
		return alias;
	}

	/**
	 * @param alias the alias to set
	 */
	public void setAlias(String alias) {
		this.alias = alias;
	}

	/**
	 * @return the table
	 */
	public SelectTable getTable() {
		return table;
	}

	/**
	 * @param table the table to set
	 */
	public void setTable(SelectTable table) {
		this.table = table;
	}
	
}
