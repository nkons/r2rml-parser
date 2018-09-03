package gr.seab.r2rml.beans;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Properties;

public interface Database {

	public Connection openConnection();

	public Statement newStatement();
	
	public void testQuery(String query);
	
	public void setProperties(Properties properties);
	
}
