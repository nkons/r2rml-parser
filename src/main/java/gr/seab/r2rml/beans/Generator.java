package gr.seab.r2rml.beans;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import gr.seab.r2rml.entities.DatabaseType;
import gr.seab.r2rml.entities.LogicalTableMapping;
import gr.seab.r2rml.entities.MappingDocument;
import gr.seab.r2rml.entities.PredicateObjectMap;
import gr.seab.r2rml.entities.Template;
import gr.seab.r2rml.entities.TermType;
import gr.seab.r2rml.entities.sql.SelectQuery;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
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
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.DC;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * Generates the resulting graph, based on the mapping document
 * @see MappingDocument
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
	private boolean storeOutputModelInTdb;
	private boolean writeReifiedModel;
	private boolean encodeURLs;
	private boolean forceUri;
	
	private Model logModel;

	private static final SimpleDateFormat xsdDateFormat = new SimpleDateFormat("yyyy-MM-dd");
		
	public Generator() {
	}
	
	public void createTriples(MappingDocument mappingDocument) {
		verbose = properties.containsKey("default.verbose") && properties.getProperty("default.verbose").contains("true");
		storeOutputModelInTdb = properties.containsKey("jena.storeOutputModelUsingTdb") && properties.getProperty("jena.storeOutputModelUsingTdb").contains("true");
		incremental = !storeOutputModelInTdb && properties.containsKey("default.incremental") && properties.getProperty("default.incremental").contains("true");
		encodeURLs = properties.containsKey("jena.encodeURLs") && properties.getProperty("jena.encodeURLs").contains("true");
		writeReifiedModel = incremental;
		forceUri = properties.containsKey("default.forceURI") && properties.getProperty("default.forceURI").contains("true");
		
		String destinationFileName = properties.getProperty("jena.destinationFileName");
		int dot = destinationFileName.lastIndexOf('.') > -1 ? destinationFileName.lastIndexOf('.') : destinationFileName.length();
		String reifiedModelFileName = destinationFileName.substring(0, dot) + "-reified" + destinationFileName.substring(dot);
		
		logModel = ModelFactory.createDefaultModel();
		String logNs = properties.getProperty("default.namespace");
		logModel.setNsPrefix("log", logNs);
		if (incremental) {
			InputStream isMap = FileManager.get().open(reifiedModelFileName);
			resultModel = ModelFactory.createDefaultModel();
			try {
				resultModel.read(isMap, null, "N-TRIPLE");
			} catch (Exception e) {
				log.error(e.toString());
				log.error("Error reading last run model. Cannot proceed with incremental, going for a full run."); // Please change property default.incremental in file r2rml.properties to false.
				resultModel.setNsPrefixes(mappingDocument.getPrefixes());
				incremental = false;
				writeReifiedModel = true;
				//System.exit(0);
			}
			
			String logFilename = properties.getProperty("default.log");
			InputStream isMapLog = FileManager.get().open(logFilename);
			try {
				logModel.read(isMapLog, properties.getProperty("default.namespace"), properties.getProperty("mapping.file.type"));
				if (incremental) log.info("Going to dump incrementally, based on log file " + properties.getProperty("default.log"));
			} catch (Exception e) {
				log.error(e.toString());
				log.error("Error reading log. Cannot proceed with incremental, going for a full run."); //Please change property default.incremental in file r2rml.properties to false.
				incremental = false;
				writeReifiedModel = true;
				//System.exit(0);
			}
			
			//remove any old statements. Set instead of a List would disallow duplicates but perform worse
			ArrayList<Statement> statementsToRemove = new ArrayList<Statement>();
			StmtIterator stmtIter = resultModel.listStatements();
			while (stmtIter.hasNext()) {
				Statement stmt = stmtIter.next();
				Resource type = stmt.getSubject().getPropertyResourceValue(RDF.type);
				if (type == null || !type.equals(RDF.Statement)) {  
					statementsToRemove.add(stmt);
				}
			}
			stmtIter.close();
			resultModel.remove(statementsToRemove); //.toArray(new Statement[statementsToRemove.size()]));
			log.info("Removed " + statementsToRemove.size() + " old statements.");
		}

		boolean executeAllMappings = false;
		int mappingsExecuted = 0;
		
		//if there are no reified statements in the result model, this means that last time incremental was set to false
		//so we need to re-create the model only reified statements
		if (incremental) {
			try {
				RSIterator rsIter = resultModel.listReifiedStatements();
				if (!rsIter.hasNext()) {
					executeAllMappings = true;
				}
				rsIter.close();
			} catch (Exception e) {
				log.error(e.toString());
				log.error("Error trying to read destination file. Forcing full mapping.");
				executeAllMappings = true;
			}
			
			try {
				Resource r = logModel.getResource(logNs + "destinationFile");
				Statement stmt1 = r.getProperty(logModel.getProperty(logNs + "destinationFileSize"));
				Long fileSizeInLogFile = Long.valueOf(stmt1.getObject().toString());
				Long actualFileSize = new Long(new File(destinationFileName).length());
				if (fileSizeInLogFile.longValue() != actualFileSize.longValue()) {
					log.info("Destination file size was found " + actualFileSize + " bytes while it should be " + fileSizeInLogFile + " bytes. Forcing full mapping.");
					executeAllMappings = true;
				}
				Statement stmt2 = r.getProperty(logModel.getProperty(logNs + "reifiedModelFileSize"));
				Long reifiedModelFileSizeInLogFile = Long.valueOf(stmt2.getObject().toString());
				Long actualReifiedModelFileSize = new Long(new File(reifiedModelFileName).length());
				if (reifiedModelFileSizeInLogFile.longValue() != actualReifiedModelFileSize.longValue()) {
					log.info("Destination reified model file size was found " + actualFileSize + " bytes while it should be " + fileSizeInLogFile + " bytes. Forcing full mapping.");
					executeAllMappings = true;
				}
			} catch (Exception e) {
				log.error(e.toString());
				log.error("Error trying to read log file. Forcing full mapping.");
				executeAllMappings = true;
			}
		}
		
		int iterCount = 0;
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

				java.sql.Statement st = db.newStatement();
				try {
					ResultSet rsSelectQueryResultsHash = st.executeQuery(logicalTableMapping.getView().getSelectQuery().getQuery());
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
				} catch (SQLException sqle) {
					log.error("Failed to execute query: " + logicalTableMapping.getView().getSelectQuery().getQuery(), sqle);
				} finally {
					try { st.close(); } catch (SQLException e) { /* ignore exception */ }
				}
				
			}
			
			ArrayList<String> subjects = new ArrayList<String>();
			if (executeMapping) {
				mappingsExecuted++;
				
				if (incremental) {
					//Since we are executing the mapping again, we are removing old statements and their respective reifications
					ArrayList<ReifiedStatement> reificationsToRemove = new ArrayList<ReifiedStatement>();
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
					ArrayList<Statement> statementsToRemove = new ArrayList<Statement>();
					for (ReifiedStatement rstmt : reificationsToRemove) {
						statementsToRemove.add(rstmt.getRequiredProperty(DC.source));
						//Also remove the statement itself
						statementsToRemove.add(rstmt.getStatement());
					}
					
					for (ReifiedStatement rstmt : reificationsToRemove) {
						resultModel.removeReification(rstmt);
					}
					
					log.info("Removing " + statementsToRemove.size() + " old statements and " + reificationsToRemove.size() + " old reified statements from source " + logicalTableMapping.getUri() + ".");
					//log.info("statementsToRemove are " + statementsToRemove.size() + " statements.");
					resultModel.remove(statementsToRemove); //.toArray(new Statement[statementsToRemove.size()]));
				}
				
				//Then insert the newly generated ones
				SelectQuery selectQuery = logicalTableMapping.getView().getSelectQuery();

				java.sql.Statement sqlStmt = db.newStatement();

				try {
					ResultSet rs = sqlStmt.executeQuery(selectQuery.getQuery());

					if (verbose) log.info("Iterating over " + selectQuery.getQuery());
					rs.beforeFirst();
					while (rs.next()) {
						Template subjectTemplate = logicalTableMapping.getSubjectMap().getTemplate();
						String resultSubject = (subjectTemplate != null) ? util.fillTemplate(subjectTemplate, rs, encodeURLs) : null;
						
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
									subjects.add(st.getSubject().getURI());
									if (incremental || writeReifiedModel) {
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
									
									if (objectTemplate != null && objectTemplate.getTermType() != TermType.AUTO) {
										//Literal o = resultModel.createLiteral(u.fillTemplate(predicateObjectMap.getObjectTemplate(), rs));
										//if (!util.isUriTemplate(resultModel, predicateObjectMap.getObjectTemplate())) {
										if (objectTemplate.getTermType() == TermType.LITERAL) {
											Literal o = null;
											
											if (predicateObjectMap.getObjectTemplate().getLanguage() == null || "".equals(predicateObjectMap.getObjectTemplate().getLanguage())) {
												String value = util.fillTemplate(objectTemplate, rs, encodeURLs);
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
												String language = predicateObjectMap.getObjectTemplate().getLanguage();
												String value = util.fillTemplate(objectTemplate, rs, encodeURLs);
												if (value != null) {
													o = resultModel.createLiteral(value, language);
													if (verbose) log.info("Adding literal triple with language: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + o.getLanguage());
												}
											}
											
											if (o != null) {
												if (forceUri && o.getString().startsWith("http")) {
													if (verbose) log.info("Changing literal to URI: <" + o.getString() + ">");
													RDFNode oToUri = resultModel.createResource(o.getString());
													
													Statement st = resultModel.createStatement(s, p, oToUri);
													subjects.add(st.getSubject().getURI());
													if (incremental || writeReifiedModel) {
														ReifiedStatement rst = resultModel.createReifiedStatement(st);
														rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
													} else {
														resultModel.add(st);
													}
												} else {
													Statement st = resultModel.createStatement(s, p, o);
													subjects.add(st.getSubject().getURI());
													if (incremental || writeReifiedModel) {
														ReifiedStatement rst = resultModel.createReifiedStatement(st);
														rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
													} else {
														resultModel.add(st);
													}
												}
											}
										} else if (objectTemplate.getTermType() == TermType.IRI) {
											if (verbose) log.info("Filling in IRI template " + objectTemplate.getText());
											String value = util.fillTemplate(objectTemplate, rs, encodeURLs);
											if (value != null) {
												RDFNode o = resultModel.createResource(value);
												if (verbose) log.info("Adding resource triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												Statement st = resultModel.createStatement(s, p, o);
												subjects.add(st.getSubject().getURI());
												if (incremental || writeReifiedModel) {
													ReifiedStatement rst = resultModel.createReifiedStatement(st);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(st);
												}
											}
										} else if (objectTemplate.getTermType() == TermType.BLANKNODE) {
											if (verbose) log.info("filling in blanknode template " + objectTemplate.getText());
											String value = util.fillTemplate(objectTemplate, rs, encodeURLs);
											if (value != null) {
												RDFNode o = resultModel.createResource(AnonId.create(value));
												if (verbose) log.info("Adding resource triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												Statement st = resultModel.createStatement(s, p, o);
												subjects.add(st.getSubject().getURI());
												if (incremental || writeReifiedModel) {
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
										
										String test = getStringValue(field, rs);
										BaseDatatype xsdDataType = findFieldDataType(field, rs);
										predicateObjectMap.setDataType(xsdDataType);

										if (test != null) {
											Literal o;
											if (predicateObjectMap.getObjectTemplate().getLanguage() == null || "".equals(predicateObjectMap.getObjectTemplate().getLanguage())) {
												
												if (predicateObjectMap.getDataType() != null) {
													o = resultModel.createTypedLiteral(test, predicateObjectMap.getDataType());
													if (verbose) log.info("Adding typed literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"^^" + predicateObjectMap.getDataType().getURI());
												} else {
													o = resultModel.createLiteral(test);
													if (verbose) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
												}
											} else {
												String language = predicateObjectMap.getObjectTemplate().getLanguage();
												o = resultModel.createLiteral(test, language);
												if (verbose) log.info("Adding triple with language: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + predicateObjectMap.getObjectTemplate().getLanguage());
											}
											
											Statement st = resultModel.createStatement(s, p, o);
											subjects.add(st.getSubject().getURI());
											if (incremental || writeReifiedModel) {
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
											if (childValue != null && !StringUtils.isNumeric(childValue)) {
												childValue = "'" + childValue + "'";
											}
											if (verbose) log.info("child value is " + childValue); 
											
											SelectQuery parentQuery;
											if (l.getSubjectMap().getSelectQuery() != null) {
												parentQuery = l.getSubjectMap().getSelectQuery();
											} else {
												parentQuery = l.getView().getSelectQuery(); //assure the select query is not null
											}
											String parentQueryText = parentQuery.getQuery();
											
											if (parentQuery.getTables().size() == 1) {
												String parentFieldName = predicateObjectMap.getRefObjectMap().getParent();
												if (mappingDocument.getDatabaseType() == DatabaseType.MYSQL) parentFieldName = parentFieldName.replaceAll("\"", ""); //in mysql, table names must not be enclosed in quotes
												boolean containsWhere = parentQueryText.toLowerCase().contains("where");
												String addition = (containsWhere ? " AND " : " WHERE ") + parentFieldName + " = " + childValue;
												int order = parentQueryText.toUpperCase().indexOf("ORDER BY");
												if (order != -1) {
													String orderCondition = parentQueryText.substring(order);
													parentQueryText = parentQueryText.substring(0, order) + addition + " " + orderCondition;
												} else {
													parentQueryText += addition;
												}
											} else {
												log.error("In the logical table mapping <" + logicalTableMapping.getUri() + ">, the SQL query that generates the parent triples in the parent logical table mapping <" + l.getUri() + "> contains results from more than one tables. " +
													" Consider using rr:tableName instead of rr:sqlQuery in the parent logical table mapping. Terminating.");
												System.exit(1);
											}
											
											if (verbose) log.info("Modified parent SQL query to " + parentQuery);
											java.sql.Statement parentSqlStmt = db.newStatement();
											ResultSet rsParent = parentSqlStmt.executeQuery(parentQueryText);
											rsParent.beforeFirst();
											while (rsParent.next()) {
												Template parentTemplate = l.getSubjectMap().getTemplate();
												String parentSubject = util.fillTemplate(parentTemplate, rsParent, encodeURLs);
												RDFNode o = resultModel.createResource(parentSubject);
												Statement st = resultModel.createStatement(s, p, o);
												if (verbose) log.info("Adding triple referring to a parent statement subject: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												subjects.add(st.getSubject().getURI());
												if (incremental || writeReifiedModel) {
													ReifiedStatement rst = resultModel.createReifiedStatement(st);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(st);
												}
											}
											rsParent.close();
											parentSqlStmt.close();
										} else {
											if (verbose) log.info("Object URIs will be the subjects of the referenced triples, created previously by the logical table mapping with the uri " + predicateObjectMap.getRefObjectMap().getParentTriplesMapUri());
											LogicalTableMapping l = mappingDocument.findLogicalTableMappingByUri(predicateObjectMap.getRefObjectMap().getParentTriplesMapUri());
											if (verbose) log.info("The logical table mapping with the uri " + l.getUri() + " has already generated "+ l.getSubjects().size() + " triples.");
											
											for (String existingStatementSubject : l.getSubjects()) {
												String existingSubjectUri = existingStatementSubject;
												RDFNode o = resultModel.createResource(existingSubjectUri);
												Statement st = resultModel.createStatement(s, p, o);
												if (verbose) log.info("Adding triple referring to an existing statement subject: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
												subjects.add(st.getSubject().getURI());
												if (incremental || writeReifiedModel) {
													ReifiedStatement rst = resultModel.createReifiedStatement(st);
													rst.addProperty(DC.source, resultModel.createResource(logicalTableMapping.getUri()));
												} else {
													resultModel.add(st);
												}
											}
										}
									}
								}
							}
						}
						iterCount++;
						if (iterCount % 10000 == 0) {
							log.info("At " + iterCount);
							//System.out.println("At " + iterCount);
						}
					}
					
					rs.close();
					sqlStmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					try { sqlStmt.close(); } catch (Exception e) {}
				}
			} else {
				log.info("Skipping triple generation from " + logicalTableMapping.getUri() + ". Nothing changed here.");
			}
			
		    logicalTableMapping.setSubjects(subjects);
		    if (verbose) log.info("Generated " + subjects.size() + " statements from table mapping <" + logicalTableMapping.getUri() + ">");
	    }
		mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //2 Generated jena model in memory
		log.info("Finished generating jena model in memory.");
		
		if (!incremental || mappingsExecuted > 0) {
			if (!storeOutputModelInTdb) {

				String destinationFileSyntax = properties.getProperty("jena.destinationFileSyntax");
				String showXmlDeclarationProperty = properties.getProperty("jena.showXmlDeclaration");
				boolean showXmlDeclaration = (destinationFileSyntax.equalsIgnoreCase("RDF/XML") || destinationFileSyntax.equalsIgnoreCase("RDF/XML-ABBREV"))
												&& showXmlDeclarationProperty.equalsIgnoreCase("true");
				
				if ((!incremental && writeReifiedModel) || incremental) {
					log.info("Generating clean model.");
					Model cleanModel = ModelFactory.createDefaultModel();
					cleanModel.setNsPrefixes(resultModel.getNsPrefixMap());
					//ArrayList<Statement> cleanStatements = new ArrayList<Statement>();
					
					RSIterator rsIter = resultModel.listReifiedStatements();
					long addedStatements = 0;
					while (rsIter.hasNext()) {
						ReifiedStatement rstmt = rsIter.next();
						//Statement st = rstmt.getStatement();
						cleanModel.add(rstmt.getStatement());
						//cleanStatements.add(rstmt.getStatement());
						addedStatements++;
						if (verbose && addedStatements % 10000 == 0) log.info("At " + addedStatements);
					}
					rsIter.close();
					
					//If no reified statements were found, try actual statements
					//if (!cleanModel.listStatements().hasNext()) {
					if (addedStatements == 0) {
						log.info("No reified statements were found, business as usual.");
						StmtIterator stmtIter = resultModel.listStatements();
						while (stmtIter.hasNext()) {
							Statement st = stmtIter.nextStatement();
							//cleanStatements.add(st);
							cleanModel.add(st);
							addedStatements++;
							if (verbose && addedStatements % 10000 == 0) log.info("At " + addedStatements);
						}
						stmtIter.close();
					}
					//log.info("Adding " + cleanStatements.size() + " statements to clean model.");
					//cleanModel.add(cleanStatements);
					//cleanStatements.clear(); //free some memory
					log.info("Writing clean model to " + destinationFileName);
					//log.info("Clean model has " + cleanModel.listStatements().toList().size() + " statements.");
					
					//Could as well be an empty model, this is why we check if it actually has any triples
					//if (cleanModel.listStatements().hasNext()) {
					if (!cleanModel.isEmpty()) {
						try {
							Calendar c0 = Calendar.getInstance();
					        long t0 = c0.getTimeInMillis();
					        
					        //Force showXmlDeclaration
					        BufferedWriter out = new BufferedWriter(new FileWriter(destinationFileName));
					        if (showXmlDeclaration) {
					        	out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
					        	out.newLine();
					        }
					        
					        cleanModel.write(out, destinationFileSyntax);
					        out.close();
					        
					        Calendar c1 = Calendar.getInstance();
					        long t1 = c1.getTimeInMillis();
					        log.info("Writing clean model to disk took " + (t1 - t0) + " milliseconds.");
						} catch (FileNotFoundException e) {
							e.printStackTrace();
						} catch (IOException e) {
							e.printStackTrace();
						}
						log.info("Clean model has " + cleanModel.size() + " statements.");
						cleanModel.close();
					} else {
						log.info("Nothing to write.");
					}
					mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //3 Wrote clean model to disk.
					//log.info("3 Wrote clean model to disk.");
				} else {
					log.info("Full run: Writing model to " + destinationFileName + ". Model has " + resultModel.size() + " statements.");
					try {
						Calendar c0 = Calendar.getInstance();
				        long t0 = c0.getTimeInMillis();
				        
				        BufferedWriter out = new BufferedWriter(new FileWriter(destinationFileName));
				        if (showXmlDeclaration) {
					        out.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>");
					        out.newLine();
				        }

				        resultModel.write(out, destinationFileSyntax);
						out.close();
						
						Calendar c1 = Calendar.getInstance();
				        long t1 = c1.getTimeInMillis();
				        log.info("Writing model to disk took " + (t1 - t0) + " milliseconds.");
						mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //3 Wrote clean model to disk
						//log.info("3 Wrote clean model to disk");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
//					StmtIterator stmtIter = resultModel.listStatements();
//					while (stmtIter.hasNext()) {
//						Statement st = stmtIter.nextStatement();
//						cleanModel.add(st);
//					}
//					stmtIter.close();
				}
				
				if (writeReifiedModel) {
					log.info("Writing reified model to " + reifiedModelFileName + ".");
					try {
						Calendar c0 = Calendar.getInstance();
				        long t0 = c0.getTimeInMillis();
				        BufferedWriter out = new BufferedWriter(new FileWriter(reifiedModelFileName));
				        resultModel.write(out, "N-TRIPLE"); //properties.getProperty("jena.destinationFileSyntax"));
				        out.close();
				        Calendar c1 = Calendar.getInstance();
				        long t1 = c1.getTimeInMillis();
				        log.info("Writing reified model to disk took " + (t1 - t0) + " milliseconds.");
					} catch (FileNotFoundException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
					log.info("Reified model has " + resultModel.size() + " statements.");
				} else {
					log.info("Not Writing reified model.");
				}
				
			} else {
				log.info("Storing model to database. Model has " + resultModel.size() + " statements.");
				Calendar c0 = Calendar.getInstance();
		        long t0 = c0.getTimeInMillis();
				//Sync start
				Dataset dataset = TDBFactory.createDataset(properties.getProperty("jena.tdb.directory"));
				dataset.begin(ReadWrite.WRITE);
				Model existingDbModel = dataset.getDefaultModel();
			    
			    log.info("Existing model has " + existingDbModel.size() + " statements.");
				
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
				stmtExistingIter.close();
				log.info("Will remove " + statementsToRemove.size() + " statements.");
				
				//then add the new ones
				Model differenceModel = resultModel.difference(existingDbModel);
				StmtIterator stmtDiffIter = differenceModel.listStatements();
				while (stmtDiffIter.hasNext()) {
					Statement stmt = stmtDiffIter.nextStatement();
					statementsToAdd.add(stmt);
				}
				stmtDiffIter.close();
				differenceModel.close();
				log.info("Will add " + statementsToAdd.size() + " statements.");
				
				existingDbModel.remove(statementsToRemove);
				existingDbModel.add(statementsToAdd);
				dataset.commit();
				dataset.end();
				
				//Sync end
				Calendar c1 = Calendar.getInstance();
		        long t1 = c1.getTimeInMillis();
		        log.info("Updating model in database took " + (t1 - t0) + " milliseconds.");
		        mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //3 Wrote clean model to tdb.
		        //log.info("3 Wrote clean model to tdb.");
			}
		} else {
			log.info("Skipping writing the output model. No changes detected.");
	        mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //3 Finished writing output model. No changes detected.
	        //log.info("3 Finished writing output model. No changes detected.");
		}
		resultModel.close();
		
		//log the results
		Calendar c0 = Calendar.getInstance();
        long t0 = c0.getTimeInMillis();
		try {
			String logFile = properties.getProperty("default.log");
			log.info("Logging results to " + new File(logFile).getAbsolutePath());
			
			//overwrite old values
			logModel = ModelFactory.createDefaultModel();
			logModel.setNsPrefix("log", logNs);

			if (verbose) log.info("Logging destination file size");
			Property pFileSize = logModel.createProperty(logNs + "destinationFileSize");
			long fileSize = new File(destinationFileName).length();
			Literal oFileSize = logModel.createLiteral(String.valueOf(fileSize));
			logModel.add(logModel.createResource(logNs + "destinationFile"), pFileSize, oFileSize);

			if (writeReifiedModel) {
				if (verbose) log.info("Logging reified model file size");
				Property pReifiedModelFileSize = logModel.createProperty(logNs + "reifiedModelFileSize");
				long reifiedModelfileSize = new File(reifiedModelFileName).length();
				Literal oReifiedModelFileSize = logModel.createLiteral(String.valueOf(reifiedModelfileSize));
				logModel.add(logModel.createResource(logNs + "destinationFile"), pReifiedModelFileSize, oReifiedModelFileSize);
				
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
					java.sql.Statement stmt = db.newStatement();
					try {
						ResultSet rsSelectQueryResultsHash =
								stmt.executeQuery(logicalTableMapping.getView().getSelectQuery().getQuery());
						Literal oSelectQueryResultsHash = logModel.createLiteral(String.valueOf(util.md5(rsSelectQueryResultsHash)));
						logModel.add(s, pSelectQueryResultsHash, oSelectQueryResultsHash);
					} catch (SQLException e) {
						log.error("Failed to execute query: " + logicalTableMapping.getView().getSelectQuery().getQuery(), e);
					} finally {
						try { stmt.close(); } catch (SQLException e) {}
					}

//					if (verbose) log.info("Logging tripleCount");
//					Property pTripleCount = logModel.createProperty(logNs + "tripleCount");
//					Literal oTripleCount = logModel.createLiteral(String.valueOf(logicalTableMapping.getTriples().size()));
//					logModel.add(s, pTripleCount, oTripleCount);
				}
			}

			if (verbose) log.info("Logging timestamp");
			Property pTimestamp = logModel.createProperty(logNs + "timestamp");
			Literal oTimestamp = logModel.createLiteral(String.valueOf(new Date()));
			logModel.add(logModel.createResource(logNs + "destinationFile"), pTimestamp, oTimestamp);
			
			BufferedWriter out = new BufferedWriter(new FileWriter(properties.getProperty("default.log")));
			logModel.write(out, properties.getProperty("mapping.file.type"));
			out.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Calendar c1 = Calendar.getInstance();
        long t1 = c1.getTimeInMillis();
        log.info("Logging took " + (t1 - t0) + " milliseconds.");
        mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //4 Finished logging.
        //log.info("4 Finished logging.");
	}

	private String getStringValue(String field, ResultSet rs) {
		String result = null;
		try {
			if (rs.getObject(field) == null) return null;

			BaseDatatype fieldDataType = findFieldDataType(field, rs);

			if (fieldDataType != null && fieldDataType.getURI().equals(XSDDatatype.XSDdate.getURI())) {
				result = xsdDateFormat.format(rs.getDate(field));
			} else {
				result = rs.getString(field);
			}
		} catch (Exception e) {
			log.error("Failed to get value as string for column " + field, e);
		}
		return result;
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
