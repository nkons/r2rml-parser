/**
 * 
 */
package gr.ekt.r2rml.beans;

import gr.ekt.r2rml.entities.Template;
import gr.ekt.r2rml.entities.sparql.LocalResultSet;

import java.sql.ResultSet;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * @author nkons
 *
 */
public interface Util {
	
	String fillTemplate(Template template, ResultSet rs);
	
	String findDatabaseType(String driver);
	
	String stripQuotes(String input);
	
	LocalResultSet sparql(Model model, String query);
	
	BaseDatatype findDataType(String dataType);
	
}
