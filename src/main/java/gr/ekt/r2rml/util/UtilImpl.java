/**
 * 
 */
package gr.ekt.r2rml.util;

import gr.ekt.r2rml.components.Template;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.rdf.model.Model;

/**
 * Collection of utility functions.
 * @author nkons
 *
 */
@Component
public class UtilImpl implements Util {
	
	private static final Logger log = LoggerFactory.getLogger(UtilImpl.class);
	
	/**
	 * Default no-argument constructor
	 */
	public UtilImpl() {
		
	}
	
	public String fillTemplate(Template template, ResultSet rs) {
		String result = template.getText();
		try {
			for (String field : template.getFields()) {
				String before = result.substring(0, result.indexOf('{'));
				String after = result.substring(result.indexOf('}') + 1);
				result = before + rs.getString(field) + after;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public String findDatabaseType(String driver) {
		
		if (driver.contains("mysql")) {
			return "mysql";
		} else if (driver.contains("postgresql")) {
			return "postgresql";
		} else {
			System.out.println("Unknown database type.");
			System.exit(1);
		}
		return null;
	}
	
	public boolean isUriTemplate(Model model, Template template) {
		String s = template.getText();
		
		for (String key : model.getNsPrefixMap().keySet()) {
			//log.info("checking if " + s + " contains '" + key + ":' or " + model.getNsPrefixMap().get(key));
			if (s.contains(key + ":") || s.contains(model.getNsPrefixMap().get(key))) return true;
		}
		
		if (s.contains("http")) {
			//log.info("returning true");
			return true;
		} else {
			//log.info("returning false");
			return false;
		}
	}
}
