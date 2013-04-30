/**
 * 
 */
package gr.ekt.r2rml.beans;

import gr.ekt.r2rml.entities.Template;
import gr.ekt.r2rml.entities.TermType;
import gr.ekt.r2rml.entities.sparql.LocalResource;
import gr.ekt.r2rml.entities.sparql.LocalResultRow;
import gr.ekt.r2rml.entities.sparql.LocalResultSet;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
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
		log.info("filling in template " + template.getText());
		String result = new String();
		if (template!= null && template.getText() != null) {
			result = template.getText();
		} else {
			result = "";
		}
		
		try {
			for (String field : template.getFields()) {
				if (field.startsWith("\"") && field.endsWith("\"")) {
					field = field.replaceAll("\"", "");
					//log.info("Cleaning. Field is now " + field);
				}
				if (rs.getString(field) != null) {
					String before = result.substring(0, result.indexOf(field));
					before = before.substring(0, before.lastIndexOf('{'));

					String after = result.substring(result.indexOf(field) + field.length());
					after = after.substring(after.indexOf('}') + 1);
					
					result = before + rs.getString(field) + after;
				} else {
					result = null;
				}
			}
			
			if (template.getTermType() == TermType.IRI && !template.isUri()) {
				//log.info("Processing URI template with namespace " + template.getText());
				try {
					result = template.getNamespace() + "/" + URLEncoder.encode(result, "UTF-8");;
				} catch (UnsupportedEncodingException e) {
					log.error("An error occurred: " + e.getMessage());
					System.exit(0);
				}
			}
			
			if (template.isUri()) {
				//log.info("Processing URI template " + template.getText());
				try {
					int r = Math.max(result.lastIndexOf('#'), result.lastIndexOf('/')) + 1;
					if (r > -1) result = result.substring(0, r) + URLEncoder.encode(result.substring(r), "UTF-8");;
				} catch (UnsupportedEncodingException e) {
					log.error("An error occurred: " + e.getMessage());
					System.exit(0);
				}
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
	
	/**
	 * returns an XSD datatype according to an XSD declaration
	 */
	public BaseDatatype findDataType(String dataType) {
		if ("anyURI".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDanyURI;
    	} else if ("base64Binary".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDbase64Binary;
    	} else if ("boolean".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDboolean;
    	} else if ("byte".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDbyte;
    	} else if ("date".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDdate;
    	} else if ("dateTime".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDdateTime;
    	} else if ("decimal".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDdecimal;
    	} else if ("double".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDdouble;
    	} else if ("duration".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDduration;
    	} else if ("ENTITY".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDENTITY;
    	} else if ("float".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDfloat;
    	} else if ("gDay".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDgDay;
    	} else if ("gMonth".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDgMonth;
    	} else if ("gMonthDay".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDgMonthDay;
    	} else if ("gYear".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDgYear;
    	} else if ("gYearMonth".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDgYearMonth;
    	} else if ("hexBinary".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDhexBinary;
    	} else if ("ID".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDID;
    	} else if ("IDREF".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDIDREF;
    	} else if ("int".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDint;
    	} else if ("integer".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDinteger;
    	} else if ("language".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDlanguage;
    	} else if ("long".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDlong;
    	} else if ("Name".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDName;
    	} else if ("NCName".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDNCName;
    	} else if ("negativeInteger".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDnegativeInteger;
    	} else if ("NMTOKEN".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDNMTOKEN;
    	} else if ("nonNegativeInteger".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDnonNegativeInteger;
    	} else if ("nonPositiveInteger".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDnonPositiveInteger;
    	} else if ("normalizedString".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDnormalizedString;
    	} else if ("NOTATION".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDNOTATION;
    	} else if ("positiveInteger".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDpositiveInteger;
    	} else if ("QName".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDQName;
    	} else if ("short".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDshort;
    	} else if ("string".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDstring;
    	} else if ("time".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDtime;
    	} else if ("token".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDtoken;
    	} else if ("unsignedByte".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDunsignedByte;
    	} else if ("unsignedInt".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDunsignedInt;
    	} else if ("unsignedLong".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDunsignedLong;
    	} else if ("unsignedShort".equalsIgnoreCase(dataType)) {
    		return XSDDatatype.XSDunsignedShort;
    	} else {
    		log.info("Found unknown datatype " + dataType);
    		System.exit(0);
    	}
		return null;
	}
	
}
