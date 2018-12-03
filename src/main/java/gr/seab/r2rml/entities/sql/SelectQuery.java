package gr.seab.r2rml.entities.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents an sql select query
 *
 */
public class SelectQuery {
	private static final Logger log = LoggerFactory.getLogger(SelectQuery.class);

	private String query;
	private ArrayList<SelectField> fields;
	private ArrayList<SelectTable> tables;
	
	private final Properties p;
	/**
	 * 
	 */
	public SelectQuery(String query, Properties p) {
		this.p = p;
		log.info("Processing query: '" + query + "'");
		if (query.indexOf(';') != -1) query = query.replace(';', ' ');
		
		this.query = query;
		this.tables = createSelectTables(query); //tables must be created first, they are needed to create the fields
		this.fields = createSelectFields(query);
	}
	
	public SelectField findFieldByNameOrAlias(String field) {
		for (SelectField f : fields) {
			if (field.equalsIgnoreCase(f.getName()) || field.equalsIgnoreCase(f.getAlias())) {
				return f;
			}
		}
		return null;
	}
	
	public SelectTable findTableByNameOrAlias(String search) {
		for (SelectTable table : tables) {
			if (search.equalsIgnoreCase(table.getName()) || search.equalsIgnoreCase(table.getAlias()) ) {
				return table;
			}
		}
		return null;
	}
	
	public ArrayList<SelectField> createSelectFields(String q) {
		ArrayList<SelectField> results = new ArrayList<SelectField>();
		
		int start = q.toUpperCase().indexOf("SELECT") + 7;
		int end = q.toUpperCase().indexOf("FROM");

		List<String> fields = splitFields(q.substring(start, end));
		List<String> processedFields = new ArrayList<String>(); 
		
		for (int i = 0; i < fields.size(); i++) {
			String strippedFieldName = StringUtils.strip(fields.get(i));
			if (createSelectFieldTable(strippedFieldName) != null) {
				processedFields.add(strippedFieldName);
			}
		}
		
		for (String field : processedFields) {
			SelectField f = new SelectField();
			f.setName(field.trim());
			f.setTable(createSelectFieldTable(field.trim()));
			f.setAlias(createAlias(field).trim());
			log.info("Adding field with: name '" + f.getName() + "', table '" + f.getTable().getName() + "', alias '" + f.getAlias() + "'");
			results.add(f);
		}
		return results;
	}
	
	public ArrayList<SelectTable> createSelectTables(String q) {
		ArrayList<SelectTable> results = new ArrayList<SelectTable>();
		
		int start = q.toUpperCase().indexOf("FROM") + 4;
		ArrayList<String> tables = new ArrayList<String>();
		
		//split in spaces
		String tokens[] = q.substring(start).split("\\s+");
		
		//if there are aliases
		if (StringUtils.containsIgnoreCase(q.substring(start), " AS ")) {
			for (int i = 0; i < tokens.length; i++) {
				if ("AS".equalsIgnoreCase(tokens[i])) {
					if (tokens[i+1].endsWith(",")) {
						tokens[i+1] = tokens[i+1].substring(0, tokens[i+1].indexOf(",")); 
					}
					tables.add(tokens[i-1] + " " + tokens[i] + " " + tokens[i+1]);
				}
			}
		} else {
			//otherwise, split in commas
			int end = StringUtils.indexOfIgnoreCase(q, "WHERE");
			if (end == -1) end = q.length();
			tables = new ArrayList<String>(Arrays.asList(q.substring(start, end).split(",")));
		}
		
		for (String table : tables) {
			SelectTable selectTable = new SelectTable();
			selectTable.setName(table.trim());
			selectTable.setAlias(createAlias(selectTable.getName()).trim());
			log.info("Adding table with: name '" + selectTable.getName() + "', alias '" + selectTable.getAlias() + "'");
			results.add(selectTable);
		}
		return results;
	}

	public SelectTable createSelectFieldTable(String field) {
		int dot = field.indexOf('.');
		if (dot == -1) {
			return tables.get(0); //If no table specified for the field, there should be only one table in the query
		} else {
			//if it is an SQL function, the table name should start after the parenthesis.
			int start = (field.indexOf("(") > -1? field.indexOf("(") + 1: 0);
			String table = field.substring(start, dot);
			return findTableByNameOrAlias(table);
		}
	}
	
	public String createAlias(String field) {
		String result = "";
		
		int as = field.toUpperCase().indexOf("AS");
		if (as == -1) {
			//When there is no aliases, Postgres returns the column, named after the function alone while Mysql will
			//name the column after the function plus the argument. i.e. in the case of sqrt(i), postgres will return sqrt
			//while mysql will return sqrt(i).
			String driver = p.getProperty("db.driver");

			if (driver.contains("postgres")) {
				int parenthesis = field.indexOf("(");
				if  (parenthesis > -1) {
					return field.substring(0, parenthesis);
				} else {
					return result;
				}
			} else {
				return field.trim();
			}
		} else {
			return field.substring(as + 2, field.length());
		}
	}

	/**
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}

	/**
	 * @param query the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	/**
	 * @return the fields
	 */
	public ArrayList<SelectField> getFields() {
		return fields;
	}

	/**
	 * @param fields the fields to set
	 */
	public void setFields(ArrayList<SelectField> fields) {
		this.fields = fields;
	}
	
	/**
	 * @return the tables
	 */
	public ArrayList<SelectTable> getTables() {
		return tables;
	}
	/**
	 * @param tables the tables to set
	 */
	public void setTables(ArrayList<SelectTable> tables) {
		this.tables = tables;
	}	
	
	protected List<String> splitFields(String fieldString) {
		List<String> tokens = new ArrayList<String>();
		int parenthesesCount = 0;
		String token = "";
		char[] chars = fieldString.toCharArray();
		for (char c : chars) {
			if (c == ',' && parenthesesCount == 0) {
				tokens.add(token);
				token = "";
			} else {
				if (c == '(') {
					parenthesesCount++;
				} else if (c == ')') {
					parenthesesCount--;
				}
				token += c;
			}
		}
		tokens.add(token);
		
		return tokens;
	}
}
