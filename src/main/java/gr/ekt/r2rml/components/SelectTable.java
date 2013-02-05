/**
 * 
 */
package gr.ekt.r2rml.components;

/**
 * A table in an sql select query
 * @see SelectQuery
 * @author nkons
 *
 */
public class SelectTable {

	private String name;
	private String alias;
	
	/**
	 * 
	 */
	public SelectTable() {
	
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
}
