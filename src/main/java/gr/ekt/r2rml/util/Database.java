/**
 * 
 */
package gr.ekt.r2rml.util;

import java.sql.Connection;
import java.sql.ResultSet;

import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.sql.SDBConnection;

/**
 * @author nkons
 *
 */
public interface Database {
	
	public Connection openConnection();
	
	public SDBConnection openJenaConnection();
	
	public ResultSet query(String query);
	
	public Store jenaStore();
}
