package gr.seab.r2rml.entities.sql;

/**
 * A select field in an sql select query
 * @see SelectQuery
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
