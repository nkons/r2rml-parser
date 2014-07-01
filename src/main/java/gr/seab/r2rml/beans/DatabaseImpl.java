/**
 * Licensed under the Creative Commons Attribution-NonCommercial 3.0 Unported 
 * License (the "License"). You may not use this file except in compliance with
 * the License. You may obtain a copy of the License at:
 * 
 *  http://creativecommons.org/licenses/by-nc/3.0/
 *  
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gr.seab.r2rml.beans;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Database functions
 * @author nkons
 * 
 */
public class DatabaseImpl implements Database {
	
	private static final Logger log = LoggerFactory.getLogger(Database.class);

	private Connection connection;
	
	private Properties properties = new Properties();
	
	private String propertiesFilename;
	
	private Util util;
	
	public DatabaseImpl() {
		
	}
	
	/**
	 * 
	 */
	public DatabaseImpl(String propertiesFilename) {
		try {
			properties.load(new FileInputStream(propertiesFilename));
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public Connection openConnection() {
		log.info("Establishing source (relational) connection.");
		if (connection == null) {
			try {
				String driver = properties.getProperty("db.driver");
				Class.forName(driver);
				String dbConnectionString = properties.getProperty("db.url");
				if (dbConnectionString == null) {
                    log.error("The property db.url cannot be empty! It must contain a valid database connection string.");
                    System.exit(1);
				}
				connection = DriverManager.getConnection(dbConnectionString, properties.getProperty("db.login"), properties.getProperty("db.password"));
			
				log.info("Established source (relational) connection.");
				return connection;
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		} else {
			return connection;
		}
		return null;
	}
	
	public ResultSet query(String query) {
		ResultSet result = null;
		
		try {
			if (connection == null) openConnection();
			
			//PreparedStatement preparedStatement = connection.prepareStatement(query);
			Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			result = statement.executeQuery(query);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
		return result;
	}
	
	/**
	 * 
	 * Create a PreparedStatement with the query string and get its metadata. If this works, the query string is ok (but nothing is executed)
	 * 
	 */
	public void testQuery(String query) {
		try {
			if (connection == null) openConnection();

			PreparedStatement preparedStatement = connection.prepareStatement(query);
			
			ResultSetMetaData m = preparedStatement.getMetaData();
			log.info ("Query is ok. Retrieves a dataset with " + m.getColumnCount() + " column(s).");
			
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(1);
		}
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
	
	public Properties getProperties() {
		return properties;
	}
	
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
}
