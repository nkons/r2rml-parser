/**
 * 
 */
package gr.ekt.r2rml.beans;


import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.StoreDesc;
import com.hp.hpl.jena.sdb.sql.SDBConnection;
import com.hp.hpl.jena.sdb.store.DatabaseType;
import com.hp.hpl.jena.sdb.store.LayoutType;

/**
 * Database functions.
 * @author nkons
 * 
 */
public class DatabaseImpl implements Database {
	
	private static final Logger log = LoggerFactory.getLogger(Database.class);

	private Connection connection;
	
	private SDBConnection jenaConnection;

	private Store store;
	
	private Properties p = new Properties();
	
	private String propertiesFilename;
	
	private Util util;
	
	/**
	 * 
	 */
	public DatabaseImpl(String propertiesFilename) {
		try {
			p.load(new FileInputStream(propertiesFilename));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public Connection openConnection() {
		log.info("Establishing source (relational) connection.");
		if (connection == null) {
			try {
				String driver = p.getProperty("db.driver");
				Class.forName(driver);
				String databaseType = util.findDatabaseType(driver);
				String dbConnectionString = "jdbc:" + databaseType + "://" + p.getProperty("db.host") + ":" + p.getProperty("db.port") + "/" + p.getProperty("db.name");
				connection = DriverManager.getConnection(dbConnectionString, p.getProperty("db.login"), p.getProperty("db.password"));
			
				log.info("Established source (relational) connection.");
				return connection;
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(0);
			}
		} else {
			return connection;
		}
		return null;
	}
	
	public SDBConnection openJenaConnection() {
		log.info("Establishing target (jena/triplestore) connection.");
		if (jenaConnection == null) {
			try {
				
				String jenaDriver = p.getProperty("jena.db.driver");
				Class.forName(jenaDriver);
				String jenaDatabaseType = util.findDatabaseType(jenaDriver);
				String jenaConnectionString = "jdbc:" + jenaDatabaseType + "://" + p.getProperty("jena.db.host") + ":" + p.getProperty("jena.db.port") + "/" + p.getProperty("jena.db.name");
				log.info("jena repository at " + jenaConnectionString);
				
				
				jenaConnection = new SDBConnection(jenaConnectionString, p.getProperty("jena.db.login"), p.getProperty("jena.db.password")) ;
				
				StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesHash, DatabaseType.MySQL) ;
				store = SDBFactory.connectStore(jenaConnection, storeDesc);
				store.getTableFormatter().create();

				log.info("Established target (jena/triplestore) connection.");
				
				return jenaConnection;
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			return jenaConnection;
		}
		return null;
	}
	
	public Store jenaStore() {
		if (store == null) {
			StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesHash, DatabaseType.MySQL) ;
			store = SDBFactory.connectStore(jenaConnection, storeDesc) ;
			return store;
		} else {
			log.info("Store is not null. Returning store");
			return store;
		}
	}
	
	public ResultSet query(String query) {
		ResultSet result = null;
		
		try {
			if (connection == null) openConnection();
			
			if (!StringUtils.containsIgnoreCase(query, "limit") && p.containsKey("db.limit")) {
				query += " LIMIT " + p.getProperty("db.limit");
			}
			
			//PreparedStatement preparedStatement = connection.prepareStatement(query);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			log.info("sql query: " + query);
			result = statement.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(0);
		}
		return result;
	}
	
	public String getPropertiesFilename() {
		return propertiesFilename;
	}
	
	public void setPropertiesFilename(String propertiesFilename) {
		this.propertiesFilename = propertiesFilename;
	}
	
	public Util getUtil() {
		return util;
	}
	
	public void setUtil(Util util) {
		this.util = util;
	}
	
}
