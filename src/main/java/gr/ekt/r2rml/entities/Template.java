/**
 * 
 */
package gr.ekt.r2rml.entities;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The template is a component of the form http://data.example.com/film/{film_id}. Encloses fields in brackets.
 * @see SubjectMap
 * @see PredicateObjectMap
 * @author nkons
 *
 */
public class Template {
	private static final Logger log = LoggerFactory.getLogger(Template.class);
	/**
	 * The initial template text
	 */
	private String text;
	
	/**
	 * The fields, enclosed in brackets. Must correspond to database fields
	 */
	private ArrayList<String> fields;
	
	/**
	 * Returns true if the template refers to a literal, false if it refers to a node.
	 */
	private boolean literal;
	
	/**
	 * Default constructor. Once called, it finds the included fields.
	 */
	public Template(String text, boolean literal) {
		this.text = text;
		this.fields = createTemplateFields();
		this.literal = literal;
		
		String msg = "Template has " + fields.size() + ((fields.size() == 1)? " field: " : " fields: ");
		for (String f : fields) {
			msg += f + ", ";
		}
		if (msg.lastIndexOf(',') > 0) {
			msg = msg.substring(0, msg.lastIndexOf(','));
		}
		log.info(msg);
	}
	
	public ArrayList<String> createTemplateFields() {
		ArrayList<String> results = new ArrayList<String>();
		String template = this.text;
		while (template.indexOf('{') != -1) {
			int from = template.indexOf('{') + 1;
			int to = template.indexOf('}');
			results.add(template.substring(from, to));
			//System.out.println("adding variable '" + template.substring(from, to) + "'");
			template = template.substring(to + 1, template.length());
		}
		return results;
	}
	
	/**
	 * @return the text
	 */
	public String getText() {
		return text;
	}
	/**
	 * @param text the text to set
	 */
	public void setText(String text) {
		this.text = text;
	}
	/**
	 * @return the fields
	 */
	public ArrayList<String> getFields() {
		return fields;
	}
	/**
	 * @param fields the fields to set
	 */
	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}
	
	public boolean isLiteral() {
		return literal;
	}
	
	public void setLiteral(boolean literal) {
		this.literal = literal;
	}
	
}
