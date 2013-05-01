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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.rdf.model.AnonId;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.vocabulary.RDF;

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
	
	public Generator() {
	}
	
	public void createTriples(MappingDocument mappingDocument) {
		verbose = properties.containsKey("default.verbose") && (properties.getProperty("default.verbose").contains("true") || properties.getProperty("default.verbose").contains("yes"));
		
		for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
			ArrayList<Statement> triples = new ArrayList<Statement>();
		    try {
				SelectQuery selectQuery = logicalTableMapping.getView().getSelectQuery();
				//log.info("About to execute " + selectQuery.getQuery());
				java.sql.ResultSet rs = db.query(selectQuery.getQuery());
				rs.beforeFirst();
				int tripleCount = 0;
				while (rs.next()) {
					Template subjectTemplate = logicalTableMapping.getSubjectMap().getTemplate();
					String resultSubject = util.fillTemplate(subjectTemplate, rs);
					
					if (resultSubject != null) {
						//if (StringUtils.isNotEmpty(logicalTableMapping.getSubjectMap().getClassUri())) {
						if (logicalTableMapping.getSubjectMap().getClassUris() != null && logicalTableMapping.getSubjectMap().getClassUris().size() > 0) {
							for (String classUri : logicalTableMapping.getSubjectMap().getClassUris()) {
								Resource s = null; //resultModel.createResource();
								if (verbose) log.info("subject termType: " + subjectTemplate.getTermType().toString());
								//we cannot have a literal as a subject, it has to be an iri or a blank node
								if (subjectTemplate.getTermType() == TermType.IRI || subjectTemplate.getTermType() == TermType.LITERAL) {
									s = resultModel.createResource(resultSubject);
								} else if (subjectTemplate.getTermType() == TermType.BLANKNODE) {
									s = resultModel.createResource(AnonId.create(resultSubject));
									if (verbose) log.info("created blank node subject with id " + s.getId());
								} else {
									s = resultModel.createResource(resultSubject);
								}
								
								Property p = RDF.type;
								Resource o = resultModel.createResource(classUri);
								Statement st = resultModel.createStatement(s, p, o);
								if (verbose) log.info("Adding triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.getURI() + ">");
								resultModel.add(st);
								triples.add(st);
							}
						}
						
						//for (int i = 0; i < logicalTableMapping.getPredicateObjectMaps()  resultPredicates.size(); i++) {
						for (PredicateObjectMap predicateObjectMap : logicalTableMapping.getPredicateObjectMaps()) {
							Resource s = null; //resultModel.createResource();
							if (verbose) log.info("subject termType: " + subjectTemplate.getTermType().toString());
							if (subjectTemplate.getTermType() == TermType.IRI || subjectTemplate.getTermType() == TermType.LITERAL) {
								s = resultModel.createResource(resultSubject);
							} else if (subjectTemplate.getTermType() == TermType.BLANKNODE) {
								s = resultModel.createResource(AnonId.create(resultSubject));
								if (verbose) log.info("created blank node subject with id " + s.getId());
							} else {
								s = resultModel.createResource(resultSubject);
							}
							
							Template objectTemplate = predicateObjectMap.getObjectTemplate();
							if (objectTemplate != null && objectTemplate.getTermType() != null) {
								log.info("object type is " + objectTemplate.getTermType().toString());
							} else {
								log.info("object type is null");
							}
							
							for (String predicate : predicateObjectMap.getPredicates()) {
								
								Property p = resultModel.createProperty(predicate);
								
								if (objectTemplate != null) {
									//Literal o = resultModel.createLiteral(u.fillTemplate(predicateObjectMap.getObjectTemplate(), rs));
									//if (!util.isUriTemplate(resultModel, predicateObjectMap.getObjectTemplate())) {
									if (!objectTemplate.isUri()) {
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
											resultModel.add(st);
											triples.add(st);
										}
									} else {
										if (verbose) log.info("filling in template " + objectTemplate.getText());
										String value = util.fillTemplate(predicateObjectMap.getObjectTemplate(), rs);
										if (value != null) {
											RDFNode o = resultModel.createResource(value);
											if (verbose) log.info("Adding resource triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
											Statement st = resultModel.createStatement(s, p, o);
											resultModel.add(st);
											triples.add(st);
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
										resultModel.add(st);
										triples.add(st);
									}
		
								}
							}
						}
					}

					tripleCount++;
					if (tripleCount % 1000 == 0) log.info("at " + tripleCount + " triples");
				}
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		    logicalTableMapping.setTriples(triples);
		    log.info("Generated " + triples.size() + " statements from table mapping <" + logicalTableMapping.getUri() + ">");
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
		try {
			Model logModel = ModelFactory.createDefaultModel();
			
			String logFile = properties.getProperty("default.log");
			log.info("Logging results to " + new File(logFile).getAbsolutePath());

			//run on the table mappings
			for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
				Resource s = logModel.createResource(logicalTableMapping.getUri());
				
				Property p1 = logModel.createProperty("tripleCount");
				Literal o1 = logModel.createLiteral(String.valueOf(logicalTableMapping.getTriples().size()));
				logModel.add(s, p1, o1);
				
				Property p2 = logModel.createProperty("timestamp");
				Literal o2 = logModel.createLiteral(String.valueOf(new Date()));
				logModel.add(s, p2, o2);
				
			}
			
			logModel.write(new FileOutputStream(properties.getProperty("default.log")), properties.getProperty("jena.destinationFileSyntax"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	BaseDatatype findFieldDataType(String field, ResultSet rs) {
		field = field.trim();
		log.info("figuring out datatype of field: " + field);
		try {
			ResultSetMetaData rsMeta = rs.getMetaData();
			log.info("table name " + rsMeta.getTableName(1));
			for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
				log.info("column name is " + rsMeta.getColumnName(i));
				if (rsMeta.getColumnName(i).equals(field)) {
					String sqlType = rsMeta.getColumnTypeName(i);
					log.info("column " + i + " with name " + rsMeta.getColumnName(i) + " is of type " + sqlType);
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
