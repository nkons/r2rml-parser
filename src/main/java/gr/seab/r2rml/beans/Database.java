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

import java.sql.Connection;
import java.sql.ResultSet;

import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.sdb.sql.SDBConnection;

public interface Database {

	public Connection openConnection();

	public SDBConnection openJenaConnection();

	public ResultSet query(String query);

	public Store jenaStore();
}
