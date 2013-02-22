/**
 * 
 */
package gr.ekt.r2rml.entities;

import java.util.ArrayList;

/**
 * Holds the information of the mapping file. It is useful for retrieving values on demand.
 * @author nkons
 *
 */
public class MappingDocument {

	private ArrayList<LogicalTableView> logicalTableViews;
	private ArrayList<LogicalTableMapping> logicalTableMappings;
	
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
	
	/**
	 * @return the logicalTableViews
	 */
	public ArrayList<LogicalTableView> getLogicalTableViews() {
		return logicalTableViews;
	}
	/**
	 * @param logicalTableViews the logicalTableViews to set
	 */
	public void setLogicalTableViews(ArrayList<LogicalTableView> logicalTableViews) {
		this.logicalTableViews = logicalTableViews;
	}
	/**
	 * @return the logicalTableMappings
	 */
	public ArrayList<LogicalTableMapping> getLogicalTableMappings() {
		return logicalTableMappings;
	}
	/**
	 * @param logicalTableMappings the logicalTableMappings to set
	 */
	public void setLogicalTableMappings(
			ArrayList<LogicalTableMapping> logicalTableMappings) {
		this.logicalTableMappings = logicalTableMappings;
	}
}
