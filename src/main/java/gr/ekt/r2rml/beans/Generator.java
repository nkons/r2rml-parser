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
package gr.ekt.r2rml.beans;

import gr.ekt.r2rml.entities.LogicalTableMapping;
import gr.ekt.r2rml.entities.MappingDocument;
import gr.ekt.r2rml.entities.PredicateObjectMap;
import gr.ekt.r2rml.entities.Template;
import gr.ekt.r2rml.entities.TermType;
import gr.ekt.r2rml.entities.sql.SelectQuery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.RSIterator;
import com.hp.hpl.jena.rdf.model.ReifiedStatement;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * Generates the resulting graph, based on the mapping document
 * @see MappingDocument
 * @author nkons
 *
 */
public class Generator {

	private static final Logger log = LoggerFactory.getLogger(Generator.class);
	
	/**
	 * The resulting model, containing the input model and all the generated triples
	 */
	private Model resultModel;
	
	private Database db;
	
	private Util util;
	
	/**
	 * The properties, as read from the properties file.
	 */
	private Properties properties;
	
	private boolean verbose;
	
	private boolean incremental;
	
	private Model logModel;
		
	public Generator() {
	}
	
	public void createTriples(MappingDocument mappingDocument) {
		incremental = properties.containsKey("default.incremental") && (properties.getProperty("default.incremental").contains("true") || properties.getProperty("default.incremental").contains("yes"));
		logModel = ModelFactory.createDefaultModel();
		String logNs = properties.getProperty("default.namespace");
		logModel.setNsPrefix("log", logNs);
		if (incremental) {
			String modelFilename = properties.getProperty("jena.destinationFileName");
			InputStream isMap = FileManager.get().open(modelFilename);
			resultModel = ModelFactory.createDefaultModel();
			try {
				resultModel.read(isMap, null, properties.getProperty("jena.destinationFileSyntax"));
			} catch (Exception e) {
				log.error("Error reading last run model. Please change property default.incremental in file r2rml.properties to false.");
				System.exit(0);
			}
			
			String logFilename = properties.getProperty("default.log");
			InputStream isMapLog = FileManager.get().open(logFilename);
			try {
				logModel.read(isMapLog, properties.getProperty("default.namespace"), properties.getProperty("mapping.file.type"));
			} catch (Exception e) {
				log.error("Error reading log. Please change property default.incremental in file r2rml.properties to false.");
				System.exit(0);
			}
			log.info("Going to dump incrementally, based on log file " + properties.getProperty("default.log"));
			
			//remove any old statements. Set instead of a List, to disallow duplicates
			Set<Statement> statementsToRemove = new HashSet<Statement>();
			StmtIterator stmtIter = resultModel.listStatements();
			while (stmtIter.hasNext()) {
				Statement stmt = stmtIter.next();
				Resource type = stmt.getSubject().getPropertyResourceValue(RDF.type);
				if (type == null || !type.equals(RDF.Statement)) {  
					statementsToRemove.add(stmt);
				}
			}
			stmtIter.close();
			log.info("Removing " + statementsToRemove.size() + " old statements.");
			resultModel.remove(statementsToRemove.toArray(new Statement[statementsToRemove.size()]));
		}
		verbose = properties.containsKey("default.verbose") && (properties.getProperty("default.verbose").contains("true") || properties.getProperty("default.verbose").contains("yes"));
		String databaseType = util.findDatabaseType(properties.getProperty("db.driver"));
		
		//if there are statements in the result model, this means that last time incremental was set to false
		//so we need to re-create the model until there are no simple statements, only reified statements
		boolean executeAllMappings = false;
		if (incremental) {
			RSIterator rsIter = resultModel.listReifiedStatements();
			if (!rsIter.hasNext()) {
				executeAllMappings = true;
			}
			rsIter.close();
		}
		
		int tripleCount = 0;
		for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
			boolean executeMapping = true;
			
			if (incremental) {
				HashMap<String, String> lastRunStatistics = new HashMap<String, String>(); 
				Resource lastRunLogicalTableMapping = logModel.getResource(logicalTableMapping.getUri());
				StmtIterator iter = lastRunLogicalTableMapping.listProperties();
				while (iter.hasNext()) {
					Statement stmt = iter.next();
					Property prop = stmt.getPredicate();
					
					RDFNode node = stmt.getObject();
					if (verbose) log.info("Found in last time log " + prop.getLocalName() + " " + node.toString());
			        lastRunStatistics.put(prop.getLocalName(), node.toString());
				}
				iter.close();
				
				//selectQueryHash logicalTableMappingHash selectQueryResultsHash tripleCount timestamp
				String selectQueryHash = util.md5(logicalTableMapping.getView().getSelectQuery().getQuery());

				String logicalTableMappingHash = util.md5(logicalTableMapping);
				
				ResultSet rsSelectQueryResultsHash = db.query(logicalTableMapping.getView().getSelectQuery().getQuery());
				String selectQueryResultsHash = util.md5(rsSelectQueryResultsHash);
				
				if (selectQueryHash.equals(lastRunStatistics.get("selectQueryHash"))
						&& logicalTableMappingHash.equals(lastRunStatistics.get("logicalTableMappingHash"))
						&& selectQueryResultsHash.equals(lastRunStatistics.get("selectQueryResultsHash"))) {
					executeMapping = false || executeAllMappings;
					if (verbose) {
						if (!executeMapping) {
							log.info("Will skip triple generation from " + logicalTableMapping.getUri() + ". Found the same (a) select query (b) logical table mapping and (c) select query results.");	
						}
					}
				}
			}
			
			ArrayList<Statement> triples = new ArrayList<Statement>();
			if (executeMapping) {
				
				Set<ReifiedStatement> reificationsToRemove = new HashSet<ReifiedStatement>();
				resultModel.listReifiedStatements();
				RSIterator rsExistingIter = resultModel.listReifiedStatements();
				while (rsExistingIter.hasNext()) {
					ReifiedStatement rstmt = rsExistingIter.next();
					Statement st = rstmt.getProperty(DC.source);
					String source = st.getObject().toString();
					if (mappingDocument.findLogicalTableMappingByUri(source) != null) {
						if (logicalTableMapping.getUri().equals(source)) {
							reificationsToRemove.add(rstmt);
						}
					} else {
						reificationsToRemove.add(rstmt);
					}
				}
				rsExistingIter.close();
				
				//Remove the reified statement itself, i.e. [] a rdf:Statement ; rdf:subject ... ; rdf:predicate ; rdf:object ... ;
				//but also remove the statements having this statement as a subject and dc:source as a property
				Set<Statement> statementsToRemove = new HashSet<Statement>();
				for (ReifiedStatement rstmt : reificationsToRemove) {
					statementsToRemove.add(rstmt.getRequiredProperty(DC.source));
				}
				
				for (ReifiedStatement rstmt : reificationsToRemove) {
					resultModel.removeReification(rstmt);
				}
				
				log.info("Removing " + reificationsToRemove.size() + " old reified statements from source " + logicalTableMapping.getUri() + ".");
				//log.info("statementsToRemove are " + statementsToRemove.size() + " statements.");
				resultModel.remove(statementsToRemove.toArray(new Statement[statementsToRemove.size()]));
				
				//Then insert the newly generated ones
			    try {
					SelectQuery selectQuery = logicalTableMapping.getView().getSelectQuery();
					java.sql.ResultSet rs = db.query(selectQuery.getQuery());
					if (verbose) log.info("Iterating over " + selectQuery.getQuery());
					rs.beforeFirst();
					while (rs.next()) {
						Template subjectTemplate = logicalTableMapping.getSubjectMap().getTemplate();
						String resultSubject = (subjectTemplate != null) ? util.fillTemplate(subjectTemplate, rs) : null;
						
						if (resultSubject != null) {
							//if (StringUtils.isNotEmpty(logicalTableMapping.getSubjectMap().getClassUri())) {
							if (logicalTableMapping.getSubjectMap().getClassUris() != null && logicalTableMapping.getSubjectMap().getClassUris().size() > 0) {
								for (String classUri : logicalTableMapping.getSubjectMap().getClassUris()) {
									Resource s = null; //resultModel.createResource();
									if (verbose) log.info("Subject termType: " + subjectTemplate.getTermType().toString());
									//we cannot have a literal as a subject, it has to be an iri or a blank node
									if (subjectTemplate.getTermType() == TermType.IRI || subjectTemplate.getTermType() == TermType.LITERAL) {
										s = resultModel.createResource(resultSubject);
									} else if (subjectTemplate.getTermType() == TermType.BLANKNODE) {
										s = resultModel.createResource(AnonId.create(resultSubject));
										if (verbose) log.info("Created blank node subject with id " + s.getId());
									} else {
										s = resultModel.createResource(resultSubject);
									}
									
									Property p = RDF.type;
									Resource o = resultModel.createResource(classUri);
									Statement st = resultModel.createStatement(s, p, o);
									if (verbose) log.info("Adding triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.getURI() + ">");
									triples.add(st);
									if (incremental) {
										ReifiedStatement rst = resultModel.createReifiedStatement(st);
										rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
									} else {
										resultModel.add(st);
									}
								}
							}
							
							//for (int i = 0; i < logicalTableMapping.getPredicateObjectMaps()  resultPredicates.size(); i++) {
							for (PredicateObjectMap predicateObjectMap : logicalTableMapping.getPredicateObjectMaps()) {
								Resource s = null; //resultModel.createResource();
								if (verbose) log.info("Subject termType: " + subjectTemplate.getTermType().toString());
								if (subjectTemplate.getTermType() == TermType.IRI || subjectTemplate.getTermType() == TermType.LITERAL) {
									s = resultModel.createResource(resultSubject);
								} else if (subjectTemplate.getTermType() == TermType.BLANKNODE) {
									s = resultModel.createResource(AnonId.create(resultSubject));
									if (verbose) log.info("Created blank node subject with id " + s.getId());
								} else {
									s = resultModel.createResource(resultSubject);
								}
								
								Template objectTemplate = predicateObjectMap.getObjectTemplate();
								if (verbose) {
									if (objectTemplate != null && objectTemplate.getTermType() != null) {
										log.info("Object type is " + objectTemplate.getTermType().toString());
									} else {
										log.info("Object type is null");
									}
								}
								
								for (String predicate : predicateObjectMap.getPredicates()) {
									
									Property p = resultModel.createProperty(predicate);
									
									if (objectTemplate != null) {
										//Literal o = resultModel.createLiteral(u.fillTemplate(predicateObjectMap.getObjectTemplate(), rs));
										//if (!util.isUriTemplate(resultModel, predicateObjectMap.getObjectTemplate())) {
										if (objectTemplate.getTermType() == TermType.LITERAL) {
											Literal o = null;
											
											if (predicateObjectMap.getLanguage() == null) {
												String value = util.fillTemplate(objectTemplate, rs);
												if (value != null)  {
													if (predicateObjectMap.getDataType() != null) {
														o = resultModel.createTypedLiteral(value, predicateObjectMap.getDataType());
														if (verbose) log.info("Adding typed literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"^^" + predicateObjectMap.getDataType().getURI());
													} else {
														o = resultModel.createLiteral(value);
														if (verbose) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
													}
												}
											} else {
												String language = util.fillTemplate(predicateObjectMap.getLanguage(), rs);
												String value = util.fillTemplate(objectTemplate, rs);
												if (value != null) {
													o = resultModel.createLiteral(value, language);
													if (verbose) log.info("Adding literal triple with language: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + o.getLanguage());
												}
											}
											
											if (o != null) {
												Statement st = resultModel.createStatement(s, p, o);
												triples.add(st);
												if (incremental) {
													ReifiedStatement rst = resultModel.createReifiedStatement(st);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(st);
												}
											}
										} else if (objectTemplate.getTermType() == TermType.IRI) {
											if (verbose) log.info("Filling in IRI template " + objectTemplate.getText());
											String value = util.fillTemplate(objectTemplate, rs);
											if (value != null) {
												RDFNode o = resultModel.createResource(value);
												if (verbose) log.info("Adding resource triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												Statement st = resultModel.createStatement(s, p, o);
												triples.add(st);
												if (incremental) {
													ReifiedStatement rst = resultModel.createReifiedStatement(st);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(st);
												}
											}
										} else if (objectTemplate.getTermType() == TermType.BLANKNODE) {
											if (verbose) log.info("filling in blanknode template " + objectTemplate.getText());
											String value = util.fillTemplate(objectTemplate, rs);
											if (value != null) {
												RDFNode o = resultModel.createResource(AnonId.create(value));
												if (verbose) log.info("Adding resource triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												Statement st = resultModel.createStatement(s, p, o);
												triples.add(st);
												if (incremental) {
													ReifiedStatement rst = resultModel.createReifiedStatement(st);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(st);
												}
											}
										}
									} else if (predicateObjectMap.getObjectColumn() != null) {
										String field = predicateObjectMap.getObjectColumn();
										if (field.startsWith("\"") && field.endsWith("\"")) {
											field = field.replaceAll("\"", "");
											//log.info("Cleaning. Field is now " + field);
										}
										
										String test = null;
										try {
											test = rs.getString(field);
											BaseDatatype xsdDataType = findFieldDataType(field, rs);
											predicateObjectMap.setDataType(xsdDataType);
										} catch (Exception e) {
											log.error(e.getMessage());
										}
										
										if (test != null) {
											Literal o;
											if (predicateObjectMap.getLanguage() == null) {
												
												if (predicateObjectMap.getDataType() != null) {
													o = resultModel.createTypedLiteral(test, predicateObjectMap.getDataType());
													if (verbose) log.info("Adding typed literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"^^" + predicateObjectMap.getDataType().getURI());
												} else {
													o = resultModel.createLiteral(test);
													if (verbose) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
												}
											} else {
												String language = util.fillTemplate(predicateObjectMap.getLanguage(), rs);
												o = resultModel.createLiteral(test, language);
												if (verbose) log.info("Adding triple with language: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + predicateObjectMap.getLanguage());
											}
											
											Statement st = resultModel.createStatement(s, p, o);
											triples.add(st);
											if (incremental) {
												ReifiedStatement rst = resultModel.createReifiedStatement(st);
												rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
											} else {
												resultModel.add(st);
											}
										}
									} else if (predicateObjectMap.getRefObjectMap() != null && predicateObjectMap.getRefObjectMap().getParentTriplesMapUri() != null) {
										if (predicateObjectMap.getRefObjectMap().getParent() != null && predicateObjectMap.getRefObjectMap().getChild() != null) {
											
											if (verbose) log.info("Object URIs will be the subjects of the referenced triples, created previously by the logical table mapping with the uri " + predicateObjectMap.getRefObjectMap().getParentTriplesMapUri() 
													+ " with a rr:joinCondition containing rr:child " + predicateObjectMap.getRefObjectMap().getChild() + " and rr:parent " + predicateObjectMap.getRefObjectMap().getParent());
											LogicalTableMapping l = mappingDocument.findLogicalTableMappingByUri(predicateObjectMap.getRefObjectMap().getParentTriplesMapUri());
											
											String childValue = rs.getString(predicateObjectMap.getRefObjectMap().getChild().replaceAll("\"", "")); //table names need to be e.g. Sport instead of "Sport", and this is why we remove the quotes
											if (verbose) log.info("child value is " + childValue); 
											
											String parentQuery = l.getSubjectMap().getSelectQuery().getQuery();
											if (l.getSubjectMap().getSelectQuery().getTables().size() == 1) {
												String parentFieldName = predicateObjectMap.getRefObjectMap().getParent();
												if (!databaseType.equals("postgresql")) parentFieldName = parentFieldName.replaceAll("\"", ""); //in mysql, table names must not be enclosed in quotes
												String addition = " WHERE " + parentFieldName + " = " + childValue;
												parentQuery += addition;
											} else {
												log.error("In the logical table mapping <" + logicalTableMapping.getUri() + ">, the SQL query that generates the parent triples in the parent logical table mapping <" + l.getUri() + "> contains results from more than one tables. " +
													" Consider using rr:tableName instead of rr:sqlQuery in the parent logical table mapping. Terminating.");
												System.exit(0);
											}
											
											if (verbose) log.info("Modified parent SQL query to " + parentQuery);
											java.sql.ResultSet rsParent = db.query(parentQuery);
											rsParent.beforeFirst();
											while (rsParent.next()) {
												Template parentTemplate = l.getSubjectMap().getTemplate();
												String parentSubject = util.fillTemplate(parentTemplate, rsParent);
												RDFNode o = resultModel.createResource(parentSubject);
												Statement newStatement = resultModel.createStatement(s, p, o);
												if (verbose) log.info("Adding triple referring to a parent statement subjetct: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												triples.add(newStatement);
												if (incremental) {
													ReifiedStatement rst = resultModel.createReifiedStatement(newStatement);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(newStatement);
												}
											}
											rsParent.close();
										} else {
											if (verbose) log.info("Object URIs will be the subjects of the referenced triples, created previously by the logical table mapping with the uri " + predicateObjectMap.getRefObjectMap().getParentTriplesMapUri());
											LogicalTableMapping l = mappingDocument.findLogicalTableMappingByUri(predicateObjectMap.getRefObjectMap().getParentTriplesMapUri());
											if (verbose) log.info("The logical table mapping with the uri " + l.getUri() + " has already generated "+ l.getTriples().size() + " triples.");
											
											for (Statement existingStatement : l.getTriples()) {
												String existingSubjectUri = existingStatement.asTriple().getSubject().getURI();
												RDFNode o = resultModel.createResource(existingSubjectUri);
												Statement newStatement = resultModel.createStatement(s, p, o);
												if (verbose) log.info("Adding triple referring to an existing statement subjetct: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												triples.add(newStatement);
												if (incremental) {
													ReifiedStatement rst = resultModel.createReifiedStatement(newStatement);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(newStatement);
												}
											}
										}
									}
								}
							}
						}
						tripleCount++;
						if (tripleCount % 1000 == 0) log.info("At " + tripleCount + " triples");
					}
					
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} else {
				log.info("Skipping triple generation from " + logicalTableMapping.getUri() + ". Nothing changed here.");
			}
			
		    logicalTableMapping.setTriples(triples);
		    if (verbose) log.info("Generated " + triples.size() + " statements from table mapping <" + logicalTableMapping.getUri() + ">");
	    }
		
		if (properties.getProperty("jena.storeOutputModelInDatabase").contains("false")) {
			log.info("Writing model to " + properties.getProperty("jena.destinationFileName") + ". Model has " + resultModel.listStatements().toList().size() + " statements.");
			try {
				resultModel.write(new FileOutputStream(properties.getProperty("jena.destinationFileName")), properties.getProperty("jena.destinationFileSyntax"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			log.info("Writing model to database. Model has " + resultModel.listStatements().toList().size() + " statements.");
			//SYNC START
			//before writing the result, check the existing
			Store store = db.jenaStore();
		    Model existingDbModel = SDBFactory.connectDefaultModel(store);
			//log.info("Existing model has " + existingDbModel.listStatements().toList().size() + " statements.");
			
			List<Statement> statementsToRemove = new ArrayList<Statement>();
			List<Statement> statementsToAdd = new ArrayList<Statement>();
			
			//first clear the ones from the old model
			StmtIterator stmtExistingIter = existingDbModel.listStatements();
			while (stmtExistingIter.hasNext()) {
				Statement stmt = stmtExistingIter.nextStatement();
				if (!resultModel.contains(stmt)) {
					statementsToRemove.add(stmt);
				}
			}
			log.info("Will remove " + statementsToRemove.size() + " statements.");
			
			//then add the new ones
			StmtIterator stmtResultIter = resultModel.listStatements();
			while (stmtResultIter.hasNext()) {
				Statement stmt = stmtResultIter.nextStatement();
				if (!existingDbModel.contains(stmt)) {
					statementsToAdd.add(stmt);
				}
			}
			log.info("Will add " + statementsToAdd.size() + " statements.");
			
			existingDbModel.remove(statementsToRemove);
			existingDbModel.add(statementsToAdd);
		}
		
		//log the results
		Calendar c0 = Calendar.getInstance();
        long t0 = c0.getTimeInMillis();
		try {
			String logFile = properties.getProperty("default.log");
			log.info("Logging results to " + new File(logFile).getAbsolutePath());
			
			//overwrite old values
			logModel = ModelFactory.createDefaultModel();
			logModel.setNsPrefix("log", logNs);

			//run on the table mappings
			for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
				Resource s = logModel.createResource(logicalTableMapping.getUri());
				
				if (verbose) log.info("Logging selectQueryHash");
				Property pSelectQueryHash = logModel.createProperty(logNs + "selectQueryHash");
				String selectQuery = logicalTableMapping.getView().getSelectQuery().getQuery();
				Literal oSelectQueryHash = logModel.createLiteral(String.valueOf(util.md5(selectQuery)));
				logModel.add(s, pSelectQueryHash, oSelectQueryHash);
				
				if (verbose) log.info("Logging logicalTableMappingHash");
				Property pLogicalTableMappingHash = logModel.createProperty(logNs + "logicalTableMappingHash");
				String logicalTableMappingHash = util.md5(logicalTableMapping);
				Literal oLogicalTableMappingHash = logModel.createLiteral(String.valueOf(logicalTableMappingHash));
				logModel.add(s, pLogicalTableMappingHash, oLogicalTableMappingHash);
				
				if (verbose) log.info("Logging selectQueryResultsHash");
				Property pSelectQueryResultsHash = logModel.createProperty(logNs + "selectQueryResultsHash");
				ResultSet rsSelectQueryResultsHash = db.query(logicalTableMapping.getView().getSelectQuery().getQuery());
				Literal oSelectQueryResultsHash = logModel.createLiteral(String.valueOf(util.md5(rsSelectQueryResultsHash)));
				logModel.add(s, pSelectQueryResultsHash, oSelectQueryResultsHash);
				
//				if (verbose) log.info("Logging tripleCount");
//				Property pTripleCount = logModel.createProperty(logNs + "tripleCount");
//				Literal oTripleCount = logModel.createLiteral(String.valueOf(logicalTableMapping.getTriples().size()));
//				logModel.add(s, pTripleCount, oTripleCount);
				
				if (verbose) log.info("Logging timestamp");
				Property pTimestamp = logModel.createProperty(logNs + "timestamp");
				Literal oTimestamp = logModel.createLiteral(String.valueOf(new Date()));
				logModel.add(s, pTimestamp, oTimestamp);
			}
			
			logModel.write(new FileOutputStream(properties.getProperty("default.log")), properties.getProperty("jena.destinationFileSyntax"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		Calendar c1 = Calendar.getInstance();
        long t1 = c1.getTimeInMillis();
        log.info("Logging took " + (t1 - t0) + " milliseconds.");
        
	}
	
	BaseDatatype findFieldDataType(String field, ResultSet rs) {
		field = field.trim();
		if (verbose) log.info("Figuring out datatype of field: " + field);
		try {
			ResultSetMetaData rsMeta = rs.getMetaData();
			if (verbose) log.info("Table name " + rsMeta.getTableName(1));
			for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
				if (verbose) log.info("Column name is " + rsMeta.getColumnName(i));
				if (rsMeta.getColumnName(i).equals(field)) {
					String sqlType = rsMeta.getColumnTypeName(i);
					if (verbose) log.info("Column " + i + " with name " + rsMeta.getColumnName(i) + " is of type " + sqlType);
					return util.findDataTypeFromSql(sqlType);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public Database getDb() {
		return db;
	}
	
	public void setDb(Database db) {
		this.db = db;
	}

	public Util getUtil() {
		return util;
	}
	
	public void setUtil(Util util) {
		this.util = util;
	}
	
	public Model getResultModel() {
		return resultModel;
	}
	
	public void setResultModel(Model resultModel) {
		this.resultModel = resultModel;
	}
	
	public Properties getProperties() {
		return properties;
	}
	
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
}
