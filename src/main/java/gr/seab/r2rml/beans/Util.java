package gr.seab.r2rml.beans;

import gr.seab.r2rml.entities.LogicalTableMapping;
import gr.seab.r2rml.entities.Template;
import gr.seab.r2rml.entities.sparql.LocalResultSet;

import java.sql.ResultSet;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import gr.seab.r2rml.entities.DatabaseType;

public interface Util {
	
	String fillTemplate(Template template, ResultSet rs, boolean encodeURLs);
	
	DatabaseType findDatabaseType(String driver);
	
	String stripQuotes(String input);
	
	LocalResultSet sparql(Model model, String query);
	
	BaseDatatype findDataType(String dataType);
	
	BaseDatatype findDataTypeFromSql(String sqlDataType);
	
	String md5(ResultSet rs);
	
	String md5(String s);
	
	String md5(LogicalTableMapping logicalTableMapping);
	
}
