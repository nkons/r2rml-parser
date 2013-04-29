package gr.ekt.r2rml.test;

import gr.ekt.r2rml.beans.Generator;
import gr.ekt.r2rml.beans.Parser;
import gr.ekt.r2rml.entities.MappingDocument;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
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
	public void testAll() {
		log.info("test all");
		LinkedHashMap<String, String[]> tests = new LinkedHashMap<String, String[]>();
		tests.put("D000-1table1column0rows", new String[]{"r2rml.ttl"});
		tests.put("D001-1table1column1row", new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D002-1table2columns1row", new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl", "r2rmld.ttl", "r2rmle.ttl", "r2rmlf.ttl", "r2rmlg.ttl", "r2rmlh.ttl", "r2rmli.ttl", "r2rmlj.ttl"});
		tests.put("D003-1table3columns1row",  new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl"});
		tests.put("D004-1table2columns1row", new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D005-1table3columns3rows2duplicates",  new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D006-1table1primarykey1column1row",  new String[]{"r2rmla.ttl"});
		tests.put("D007-1table1primarykey2columns1row",  new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl", "r2rmld.ttl", "r2rmle.ttl", "r2rmlf.ttl", "r2rmlg.ttl", "r2rmlh.ttl"});
		tests.put("D008-1table1compositeprimarykey3columns1row", new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl"});
		tests.put("D009-2tables1primarykey1foreignkey",  new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl", "r2rmld.ttl"});
		tests.put("D010-1table1primarykey3colums3rows",  new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl"});
		tests.put("D011-M2MRelations",  new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D012-2tables2duplicates0nulls", new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl", "r2rmld.ttl", "r2rmle.ttl"});
		tests.put("D013-1table1primarykey3columns2rows1nullvalue",  new String[]{"r2rmla.ttl"});
		tests.put("D014-3tables1primarykey1foreignkey",  new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl", "r2rmld.ttl"});
		tests.put("D015-1table3columns1composityeprimarykey3rows2languages", new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D016-1table1primarykey10columns3rowsSQLdatatypes",  new String[]{"r2rmla.ttl", "r2rmlb.ttl", "r2rmlc.ttl", "r2rmld.ttl", "r2rmle.ttl"});
		tests.put("D017-I18NnoSpecialChars",  new String[]{});
		tests.put("D018-1table1primarykey2columns3rows", new String[]{"r2rmla.ttl"});
		tests.put("D019-1table1primarykey3columns3rows",  new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D020-1table1column5rows",  new String[]{"r2rmla.ttl", "r2rmlb.ttl"});
		tests.put("D021-2tables2primarykeys1foreignkeyReferencesAllNulls", new String[]{});
		tests.put("D022-2tables1primarykey1foreignkeyReferencesNoPrimaryKey",  new String[]{});
		tests.put("D023-2tables2primarykeys2foreignkeysReferencesToNon-primarykeys", new String[]{});
		tests.put("D024-2tables2primarykeys1foreignkeyToARowWithSomeNulls",  new String[]{});
		tests.put("D025-3tables3primarykeys3foreignkeys", new String[]{});

		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		
		int counter = 0;
		for (String key : tests.keySet()) {
			if (counter == 9) { //(counter > 4 && counter < 6) {
				String folder = "src/test/resources/" + key + "/";
				initialiseSourceDatabase(folder + "create.sql");
				
				for (String mappingFile : tests.get(key)) {
					//Override property file
					Parser parser = (Parser) context.getBean("parser");
					Properties p = parser.getProperties();
						mappingFile = folder + mappingFile;
						if (new File(mappingFile).exists()) {
							p.setProperty("mapping.file", mappingFile);
						} else {
							log.error("File " + mappingFile + " does not exist.");
						}
						p.setProperty("jena.destinationFileName", mappingFile.substring(0, mappingFile.indexOf(".") + 1) + "nq");
					parser.setProperties(p);
					MappingDocument mappingDocument = parser.parse();
			
					Generator generator = (Generator) context.getBean("generator");
					generator.setProperties(parser.getProperties());
					generator.setResultModel(parser.getResultModel());
					log.info("--- generating " + p.getProperty("jena.destinationFileName") + " from " + mappingFile + " ---");
					generator.createTriples(mappingDocument);
				}
			}
			counter++;
		}
		context.close();
	}
	
	@Test
	public void test000() {
		log.info("test 000");
		initialiseSourceDatabase("src/test/resources/D000-1table1column0rows/create.sql");
		
		//Override property file
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		Parser parser = (Parser) context.getBean("parser");
		Properties p = parser.getProperties();
			p.setProperty("mapping.file", "src/test/resources/D000-1table1column0rows/r2rml.ttl");
		parser.setProperties(p);
		
		MappingDocument mappingDocument = parser.parse();

		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(parser.getProperties());
		generator.setResultModel(parser.getResultModel());
		generator.createTriples(mappingDocument);
		
		context.close();
	}
		
	@Test
	public void testSingle() {
		log.info("test single");
		String folder = "src/test/resources/D003-1table3columns1row/";
		initialiseSourceDatabase(folder + "create.sql");
		
		//Override property file
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		Parser parser = (Parser) context.getBean("parser");
		Properties p = parser.getProperties();
			p.setProperty("mapping.file", folder + "r2rmlj.ttl");
			p.setProperty("jena.destinationFileName", "r2rmlj.nq");
		parser.setProperties(p);
		MappingDocument mappingDocument = parser.parse();

		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(parser.getProperties());
		generator.setResultModel(parser.getResultModel());
		generator.createTriples(mappingDocument);
		
		context.close();
	}
	
	/**
	 * Drops and re-creates source database
	 */
	private void initialiseSourceDatabase(String createFile) {
		if (connection == null)
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
