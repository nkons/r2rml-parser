/**
 * 
 */
package gr.ekt.r2rml.entities.sparql;

/**
 * @author nkons
 *
 */
public class LocalTriple {

	private LocalResource subject;
	private LocalResource property;
	private LocalResource object;
	
	/**
	 * 
	 */
	public LocalTriple(LocalResource subject, LocalResource property, LocalResource object) {
		this.subject = subject;
		this.property = property;
		this.object = object;
	}
	
	/**
	 * @return the subject
	 */
	public LocalResource getSubject() {
		return subject;
	}
	/**
	 * @param subject the subject to set
	 */
	public void setSubject(LocalResource subject) {
		this.subject = subject;
	}
	/**
	 * @return the property
	 */
	public LocalResource getProperty() {
		return property;
	}
	/**
	 * @param property the property to set
	 */
	public void setProperty(LocalResource property) {
		this.property = property;
	}
	/**
	 * @return the object
	 */
	public LocalResource getObject() {
		return object;
	}
	/**
	 * @param object the object to set
	 */
	public void setObject(LocalResource object) {
		this.object = object;
	}
	
}
