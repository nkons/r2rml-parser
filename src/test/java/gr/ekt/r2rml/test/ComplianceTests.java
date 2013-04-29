package gr.ekt.r2rml.test;

import gr.ekt.r2rml.beans.Generator;
import gr.ekt.r2rml.beans.Parser;
import gr.ekt.r2rml.entities.MappingDocument;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.transaction.TransactionConfiguration;

@ContextConfiguration(locations = { "classpath:test-context.xml" })
@RunWith(SpringJUnit4ClassRunner.class)
@TransactionConfiguration(defaultRollback = false)
public class ComplianceTests {

	private static final Logger log = LoggerFactory.getLogger(ComplianceTests.class);

	private Connection connection;

	@Test
	public void test000() {
		log.info("test 000");
		
		//Override property file
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		Parser parser = (Parser) context.getBean("parser");
		Properties p = parser.getProperties();
			p.setProperty("mapping.file", "src/test/resources/D000-1table1column0rows/r2rml.ttl");
		parser.setProperties(p);
		
		initialiseSourceDatabase("src/test/resources/D000-1table1column0rows/create.sql");
		MappingDocument mappingDocument = parser.parse();

		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(parser.getProperties());
		generator.setResultModel(parser.getResultModel());
		
		//Actually do the output
		generator.createTriples(mappingDocument);
		
		context.close();
	}
	
	/**
	 * 
	 */
	@Test
	public void test001() {
		log.info("test 001");
		
		//Override property file
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		Parser parser = (Parser) context.getBean("parser");
		Properties p = parser.getProperties();
			p.setProperty("mapping.file", "src/test/resources/D001-1table1column1row/r2rmlb.ttl");
			p.setProperty("jena.destinationFileName", "mappedb.nq");
		parser.setProperties(p);
		
		initialiseSourceDatabase("src/test/resources/D001-1table1column1row/create.sql");
		MappingDocument mappingDocument = parser.parse();

		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(parser.getProperties());
		generator.setResultModel(parser.getResultModel());
		
		//Actually do the output
		generator.createTriples(mappingDocument);
		
		context.close();
	}

	/**
	 * Drops and re-creates source database
	 */
	private void initialiseSourceDatabase(String createFile) {
		openConnection();
		
		String createQuery = fileContents(createFile);

		queryNoResultSet("DROP SCHEMA public CASCADE");
		queryNoResultSet("CREATE SCHEMA public");
		
		queryNoResultSet(createQuery);
	}
	
	private int queryNoResultSet(String query) {
		int rowsAffected = 0;
		try {
			if (connection == null)
				openConnection();

			Statement statement = connection.createStatement();
			log.info("sql query: " + query);
			rowsAffected = statement.executeUpdate(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rowsAffected;
	}
	
	private ResultSet query(String query) {
		ResultSet result = null;

		try {
			if (connection == null)
				openConnection();

			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			log.info("sql query: " + query);
			result = statement.executeQuery(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	private String fileContents(String filePath) {
		try {
			FileInputStream fi = new FileInputStream(filePath);

			StringBuffer contents = new StringBuffer("");
			int ch;
			while ((ch = fi.read()) != -1)
				contents.append((char) ch);
			fi.close();

			return contents.toString();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private void openConnection() {
		connection = null;

		try {
			Class.forName("org.postgresql.Driver");
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/test", "postgres", "postgres");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

}
