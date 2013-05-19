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

import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds the information of the mapping file. It is useful for retrieving values on demand.
 * @author nkons
 *
 */
public class MappingDocument {
	private static final Logger log = LoggerFactory.getLogger(MappingDocument.class);

	private LinkedList<LogicalTableView> logicalTableViews;
	private LinkedList<LogicalTableMapping> logicalTableMappings;
	
	/**
	 * 
	 */
	public MappingDocument() {
	}
	
	public LogicalTableView findLogicalTableViewByUri(String uri) {
		for (LogicalTableView logicalTableView : logicalTableViews) {
			if (uri.equalsIgnoreCase(logicalTableView.getUri())) {
				return logicalTableView;
			}
		}
		return null;
	}
	
	public LogicalTableMapping findLogicalTableMappingByUri(String uri) {
		for (LogicalTableMapping logicalTableMapping : logicalTableMappings) {
			if (uri.equalsIgnoreCase(logicalTableMapping.getUri())) {
				return logicalTableMapping;
			}
		}
		return null;
	}
	
	public LogicalTableView findLogicalTableViewByQuery(String query) {
		log.info("Searching for query " + query);
		for (LogicalTableView logicalTableView : logicalTableViews) {
			if (query.equalsIgnoreCase(logicalTableView.getSelectQuery().getQuery())) {
				log.info("Found match");
				return logicalTableView;
			}
		}
		return null;
	}
	
	/**
	 * @return the logicalTableViews
	 */
	public LinkedList<LogicalTableView> getLogicalTableViews() {
		return logicalTableViews;
	}
	/**
	 * @param logicalTableViews the logicalTableViews to set
	 */
	public void setLogicalTableViews(LinkedList<LogicalTableView> logicalTableViews) {
		this.logicalTableViews = logicalTableViews;
	}
	/**
	 * @return the logicalTableMappings
	 */
	public LinkedList<LogicalTableMapping> getLogicalTableMappings() {
		return logicalTableMappings;
	}
	/**
	 * @param logicalTableMappings the logicalTableMappings to set
	 */
	public void setLogicalTableMappings(LinkedList<LogicalTableMapping> logicalTableMappings) {
		this.logicalTableMappings = logicalTableMappings;
	}
}
