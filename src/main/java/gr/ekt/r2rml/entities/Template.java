/**
 * 
 */
package gr.ekt.r2rml.entities;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Model;

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
	 * Returns literal, blank node or iri.
	 */
	private TermType termType;
	
	/**
	 * Is used as a prefix, in case of IRIs.
	 */
	private String namespace;

	/**
	 * The model.
	 */
	private Model model;
	
	/**
	 * Default constructor. Once called, it finds the included fields.
	 */
	public Template(String text, TermType termType, String namespace, Model model) {
		this.text = text;
		this.fields = createTemplateFields();
		this.termType = termType;
		this.namespace = namespace;
		this.model = model;
		
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
		log.info("\\{");
		template = template.replaceAll("\\\\\\{", " "); //remove escaped braces \{ that could mix things
		log.info("new template " + template);
		while (template.indexOf('{') != -1) {
			int from = template.indexOf('{') + 1;
			int to = template.indexOf('}');
			results.add(template.substring(from, to));
			//System.out.println("adding variable '" + template.substring(from, to) + "'");
			template = template.substring(to + 1, template.length());
		}
		return results;
	}
	
	public boolean isUri() {
		String s = text;
		
		for (String key : model.getNsPrefixMap().keySet()) {
			//log.info("checking if " + s + " contains '" + key + ":' or " + model.getNsPrefixMap().get(key));
			if (s.contains(key + ":") || s.contains(model.getNsPrefixMap().get(key))) return true;
		}
		
		if (s.contains("http")) {
			return true;
		} else {
			return false;
		}
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
	
	public TermType getTermType() {
		return termType;
	}
	
	public void setTermType(TermType termType) {
		this.termType = termType;
	}
	
	public String getNamespace() {
		return namespace;
	}
	
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	public Model getModel() {
		return model;
	}
	
	public void setModel(Model model) {
		this.model = model;
	}
		
}
