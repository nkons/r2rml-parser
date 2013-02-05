/**
 * 
 */
package gr.ekt.r2rml.util;

import gr.ekt.r2rml.components.Template;

import java.sql.ResultSet;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * @author nkons
 *
 */
public interface Util {
	
	public String fillTemplate(Template template, ResultSet rs);
	
	public String findDatabaseType(String driver);
	
	public boolean isUriTemplate(Model model, Template template);
}
