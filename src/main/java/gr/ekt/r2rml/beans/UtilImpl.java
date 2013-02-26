/**
 * 
 */
package gr.ekt.r2rml.beans;

import gr.ekt.r2rml.entities.Template;
import gr.ekt.r2rml.entities.sparql.LocalResource;
import gr.ekt.r2rml.entities.sparql.LocalResultRow;
import gr.ekt.r2rml.entities.sparql.LocalResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * Collection of utility functions.
 * @author nkons
 *
 */
public class UtilImpl implements Util {
	
	private static final Logger log = LoggerFactory.getLogger(UtilImpl.class);
	
	/**
	 * Default no-argument constructor
	 */
	public UtilImpl() {
		log.info("Init Util.");
	}
	
	public String fillTemplate(Template template, ResultSet rs) {
		String result = template.getText();
		try {
			for (String field : template.getFields()) {
				if (field.startsWith("\"") && field.endsWith("\"")) {
					field = field.replaceAll("\"", "");
					log.info("Cleaning. Field is now " + field);
				}
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

	public String stripQuotes(String input) {
		return input.replaceAll("\"", "").trim();
	}
	
	public LocalResultSet sparql(Model model, String query) {
		
		//if the query does not have the prefixes in the beginning, include them
		if (!query.toLowerCase().startsWith("prefix")) {
			Map<String, String> prefixMap = model.getNsPrefixMap();
			if (!prefixMap.isEmpty()) {
				String prefixes = new String();
				for (String prefix : prefixMap.keySet()) {
					prefixes += "PREFIX " + prefix + ": <" + prefixMap.get(prefix) + "> \n";
				}
				query = prefixes + query;
			}
		}
		
		LocalResultSet result = new LocalResultSet();
		ArrayList<LocalResultRow> resultRows = new ArrayList<LocalResultRow>();
		
		log.info("creating query\n" + query);
		Query q = QueryFactory.create(query);
		QueryExecution qexec = QueryExecutionFactory.create(q, model);
		com.hp.hpl.jena.query.ResultSet sparqlResults = qexec.execSelect();
		result.setVariables(sparqlResults.getResultVars());

		while (sparqlResults.hasNext()) {
			QuerySolution qs = sparqlResults.next();
			//log.info("Moving to row " + sparqlResults.getRowNumber() + ": " + qs.toString());
			
			ArrayList<LocalResource> localResources = new ArrayList<LocalResource>();
			for (String variable : sparqlResults.getResultVars()) {
				try {
					RDFNode r = qs.get(variable);
					if (r.isResource()) {
						Resource resource = qs.getResource(variable);
						LocalResource localResource = new LocalResource(resource);
						localResources.add(localResource);
					} else if (r.isLiteral()) {
						String value = r.asNode().getLiteralLexicalForm();
						LocalResource localResource = new LocalResource(value);
						localResources.add(localResource);
					}
				} catch (Exception e) {
					log.error("Exception caught");
					//e.printStackTrace();
				}
			}
			LocalResultRow localResultRow = new LocalResultRow();
			localResultRow.setResources(localResources);
			resultRows.add(localResultRow);
		}
		result.setRows(resultRows);
		
		log.info("Sparql query result has the followinng " + result.getRows().size() + " rows: [");
		for (LocalResultRow row : result.getRows()) {
			String t = new String();
    		for (LocalResource resource : row.getResources()) {
    			t += resource.getLocalName() + " ";
    		}
    		log.info(t);
    	}
		log.info("]");
		
		return result;
	}
}
