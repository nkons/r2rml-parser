package gr.seab.r2rml.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RSIterator;
import com.hp.hpl.jena.rdf.model.ReifiedStatement;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.util.FileManager;

import gr.seab.r2rml.beans.Database;
import gr.seab.r2rml.beans.Generator;
import gr.seab.r2rml.beans.Parser;
import gr.seab.r2rml.beans.Util;
import gr.seab.r2rml.entities.MappingDocument;
import gr.seab.r2rml.entities.sparql.LocalResultSet;

@ContextConfiguration(locations = { "classpath:test-context.xml" })
@RunWith(SpringJUnit4ClassRunner.class)
@TransactionConfiguration(defaultRollback = false)
public class ComplianceTests {

	private static final Logger log = LoggerFactory.getLogger(ComplianceTests.class);

	private Connection connection;
	
	private static Properties properties = new Properties();

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
			if (counter > 2 && counter < 26) {
				String folder = "src/test/resources/postgres/" + key + "/";
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
	public void testSingle() {
		log.info("test single. Careful, database 'test' will be erased and re-created!");
		String folder = "src/test/resources/postgres/D014-3tables1primarykey1foreignkey/";
		initialiseSourceDatabase(folder + "create.sql");
		
		//Load property file
		try {
			properties.load(new FileInputStream("src/test/resources/test.properties"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		//Override certain properties
		properties.setProperty("mapping.file", folder + "r2rmla.ttl");
		properties.setProperty("jena.destinationFileName", folder + "r2rmla.nq");
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		Database db = (Database) context.getBean("db");
		db.setProperties(properties);
		
		Parser parser = (Parser) context.getBean("parser");
		parser.setProperties(properties);
		MappingDocument mappingDocument = parser.parse();

		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(parser.getProperties());
		generator.setResultModel(parser.getResultModel());
		generator.createTriples(mappingDocument);
		
		context.close();
	}
	
	@Test
	public void testSparqlQuery() {
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("test-context.xml");
		Util util = (Util) context.getBean("util");
		
		Model model = ModelFactory.createDefaultModel();
		String modelFilename = "dump1-epersons.rdf";
		InputStream isMap = FileManager.get().open(modelFilename);
		try {
			model.read(isMap, null, "N3");
		} catch (Exception e) {
			log.error("Error reading model.");
			System.exit(0);
		}
		String query = "SELECT ?x ?z WHERE {?x dc:source ?z} ";
		LocalResultSet rs = util.sparql(model, query);
		log.info("found " + String.valueOf(rs.getRows().size()));
		
		context.close();
	}
	
	@Test
	public void createModelFromReified() {
		Model model = ModelFactory.createDefaultModel();
		String modelFilename = "example.rdf";
		InputStream isMap = FileManager.get().open(modelFilename);
		try {
			model.read(isMap, null, "N3");
		} catch (Exception e) {
			log.error("Error reading model.");
			System.exit(0);
		}
		
		ArrayList<Statement> stmtToAdd = new ArrayList<Statement>();
		Model newModel = ModelFactory.createDefaultModel();
		RSIterator rsIter = model.listReifiedStatements();
		while (rsIter.hasNext()) {
			ReifiedStatement rstmt = rsIter.next();
			stmtToAdd.add(rstmt.getStatement());
		}
		rsIter.close();
		newModel.add(stmtToAdd.toArray(new Statement[stmtToAdd.size()]));
		
		log.info("newModel has " + newModel.listStatements().toList().size() + " statements");
	}
	
	/**
	 * Drops and re-creates source database
	 */
	private void initialiseSourceDatabase(String createFile) {
		if (connection == null)
			openConnection();
		
		String createQuery = fileContents(createFile);

		//queryNoResultSet("DROP SCHEMA public CASCADE");
		//queryNoResultSet("CREATE SCHEMA public");
		
                queryNoResultSet("DROP USER root CASCADE");
                queryNoResultSet("CREATE USER root IDENTIFIED BY 1234");
                queryNoResultSet("GRANT CREATE SESSION TO root");
                queryNoResultSet("GRANT CREATE TABLE TO root");
                queryNoResultSet("GRANT CREATE TABLESPACE TO root");
                queryNoResultSet("ALTER USER root QUOTA 10m ON system");
                
                String[] tablesToClear = {"Person", "Student", "Student_Sport", "Sport", "Country", "Info", "IOUs", "Lives",
                    "DEPT", "EMP", "LIKES", "Country", "Patient", "植物", "成分", "Employee", "Target", "Source", "Addresses", "Department",
                    "People", "Projects", "TaskAssignments"};
                for (String tableToClear : tablesToClear) {
                    try {
                        queryNoResultSet("DROP TABLE \"" + tableToClear + "\" CASCADE CONSTRAINTS");
                        log.info("Success");
                    } catch (Exception e) {
                        log.error("Error.");
                    }
                }
                                                                                        
                //Workaround for Oracle
                String[] queries = createQuery.split(";");
                for (String query: queries) {
                    queryNoResultSet(query);
                }
	}
	
	private int queryNoResultSet(String query) {
		int rowsAffected = 0;
		try {
			if (connection == null)
				openConnection();

			java.sql.Statement statement = connection.createStatement();
			log.info("sql query: " + query);
			rowsAffected = statement.executeUpdate(query);
		} catch (Exception e) {
			//e.printStackTrace();
                    log.error(e.getMessage());
		}
		return rowsAffected;
	}
	
	private ResultSet query(String query) {
		ResultSet result = null;

		try {
			if (connection == null)
				openConnection();

			java.sql.Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			log.info("sql query: " + query);
			result = statement.executeQuery(query);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Test
	public void loadPersons() {
		if (connection == null)
			openConnection();

		for (int i = 94300; i < 500000; i++) {
			String q1 = "INSERT INTO eperson (eperson_id, email, password, salt, digest_algorithm, firstname, lastname, can_log_in, require_certificate, self_registered, last_active, sub_frequency, phone, netid, language) " +
					"VALUES (" + i +", 'nkons" + i + "@live.com', 'aa07c370f18e6306d481e29d04d28ea322f3ac5d746bd1122b4907518b37875b59283054d9e91fd049f39df2223ba3feb62f9cc2923e5614503d0b9b191d6606', '2d0155aa63818899d177ff988ddde7c5', 'SHA-512', '" + randomString() + "', '" + randomString() + "', 'true', 'false', 'false', NULL, NULL, NULL, NULL, 'en');";
			String q2 = "INSERT INTO epersongroup2eperson (id, eperson_group_id, eperson_id) VALUES (" + i + ", 1, " + i + ");";
		
			try {
				java.sql.Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
				int a = statement.executeUpdate(q1);
				int b = statement.executeUpdate(q2);
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (i % 1000 == 0)
				log.info("At " + i);
		}
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
			//connection = DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521", "system", "dba");
            //connection = DriverManager.getConnection("jdbc:postgresql://83.212.115.187:5432/ebooks-dspace6", "postgres", "postgres");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	static String randomString() {
		String palette = "abcdefghijklm  nopqrstuvwxyz";
		String s = new String();

		int size = new Double((Math.random() * 50) + 2).intValue();
		for (int i = 0; i < size; i++) {
			int location = new Double(Math.random() * palette.length())
					.intValue();
			s += palette.charAt(location);
		}
		// log.info("random word " + s);
		return s.trim();
	}
}
