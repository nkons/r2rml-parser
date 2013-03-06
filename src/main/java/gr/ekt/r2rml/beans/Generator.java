package gr.ekt.r2rml.beans;

import gr.ekt.r2rml.entities.LogicalTableMapping;
import gr.ekt.r2rml.entities.MappingDocument;
import gr.ekt.r2rml.entities.PredicateObjectMap;
import gr.ekt.r2rml.entities.sql.SelectQuery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private Properties p;
	
	private boolean verbose;
	
	public Generator() {
	}
	
	public void createTriples(MappingDocument mappingDocument) {
		verbose = p.containsKey("default.verbose") && (p.getProperty("default.verbose").contains("true") || p.getProperty("default.verbose").contains("yes"));
		
		for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
			ArrayList<Statement> triples = new ArrayList<Statement>();
		    try {
				SelectQuery selectQuery = logicalTableMapping.getView().getSelectQuery();
				//log.info("About to execute " + selectQuery.getQuery());
				java.sql.ResultSet rs = db.query(selectQuery.getQuery());
				rs.beforeFirst();
				int tripleCount = 0;
				while (rs.next()) {
					String resultSubject = util.fillTemplate(logicalTableMapping.getSubjectMap().getTemplate(), rs);
					
					if (resultSubject != null) {
						if (StringUtils.isNotEmpty(logicalTableMapping.getSubjectMap().getClassUri())) {
							Resource s = resultModel.createResource(resultSubject);
							Property p = RDF.type;
							Resource o = resultModel.createResource(logicalTableMapping.getSubjectMap().getClassUri());
							Statement st = resultModel.createStatement(s, p, o);
							if (verbose) log.info("Adding triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.getURI() + ">");
							resultModel.add(st);
							triples.add(st);
						}
						
						//for (int i = 0; i < logicalTableMapping.getPredicateObjectMaps()  resultPredicates.size(); i++) {
						for (PredicateObjectMap predicateObjectMap : logicalTableMapping.getPredicateObjectMaps()) {
							Resource s = resultModel.createResource(resultSubject);
							Property p = resultModel.createProperty(predicateObjectMap.getPredicate());
							
							if (predicateObjectMap.getObjectTemplate() != null) {
								//Literal o = resultModel.createLiteral(u.fillTemplate(predicateObjectMap.getObjectTemplate(), rs));
								//if (!util.isUriTemplate(resultModel, predicateObjectMap.getObjectTemplate())) {
								if (!predicateObjectMap.getObjectTemplate().isUri()) {
									Literal o = null;
									if (StringUtils.isBlank(predicateObjectMap.getLanguage())) {
										String value = util.fillTemplate(predicateObjectMap.getObjectTemplate(), rs);
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
										String value = util.fillTemplate(predicateObjectMap.getObjectTemplate(), rs);
										if (value != null) {
											o = resultModel.createLiteral(value, predicateObjectMap.getLanguage());
											if (verbose) log.info("Adding literal triple with language: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + o.getLanguage());
										}
									}
									
									if (o != null) {
										Statement st = resultModel.createStatement(s, p, o);
										resultModel.add(st);
										triples.add(st);
									}
								} else {
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
								String test = rs.getString(field);
								if (test != null) {
									Literal o;
									if (StringUtils.isBlank(predicateObjectMap.getLanguage())) {
										
										if (predicateObjectMap.getDataType() != null) {
											o = resultModel.createTypedLiteral(test, predicateObjectMap.getDataType());
											if (verbose) log.info("Adding typed literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"^^" + predicateObjectMap.getDataType().getURI());
										} else {
											o = resultModel.createLiteral(test);
											if (verbose) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
										}
										
									} else {
										o = resultModel.createLiteral(test, predicateObjectMap.getLanguage());
										if (verbose) log.info("Adding triple with language: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + predicateObjectMap.getLanguage());
									}
									
									Statement st = resultModel.createStatement(s, p, o);
									resultModel.add(st);
									triples.add(st);
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
		
		if (p.getProperty("jena.storeOutputModelInDatabase").contains("false")) {
			log.info("Writing model to " + p.getProperty("jena.destinationFileName") + ". Model has " + resultModel.listStatements().toList().size() + " statements.");
			try {
				resultModel.write(new FileOutputStream(p.getProperty("jena.destinationFileName")), p.getProperty("jena.destinationFileSyntax"));
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
			
			String logFile = p.getProperty("default.log");
			log.info("Writing results to " + new File(logFile).getAbsolutePath());

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
			
			logModel.write(new FileOutputStream(p.getProperty("default.log")), p.getProperty("jena.destinationFileSyntax"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
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
	
	public Properties getP() {
		return p;
	}
	
	public void setP(Properties p) {
		this.p = p;
	}
	
}
