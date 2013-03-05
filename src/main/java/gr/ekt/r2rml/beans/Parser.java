/**
 * 
 */
package gr.ekt.r2rml.beans;

import gr.ekt.r2rml.entities.LogicalTableMapping;
import gr.ekt.r2rml.entities.LogicalTableView;
import gr.ekt.r2rml.entities.MappingDocument;
import gr.ekt.r2rml.entities.PredicateObjectMap;
import gr.ekt.r2rml.entities.SubjectMap;
import gr.ekt.r2rml.entities.Template;
import gr.ekt.r2rml.entities.TermType;
import gr.ekt.r2rml.entities.sparql.LocalResultSet;
import gr.ekt.r2rml.entities.sql.SelectQuery;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.sdb.SDBFactory;
import com.hp.hpl.jena.sdb.Store;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.vocabulary.RDF;

/**
 * Parses a valid R2RML Mapping Document and produces the associated triples.
 * @author nkons
 *
 */
public class Parser {
	private static final Logger log = LoggerFactory.getLogger(Parser.class);

	/**
	 * The mapping definitions
	 */
	private Model mapModel;
	
	/**
	 * The resulting model, containing the input model and all the generated triples
	 */
	private Model resultModel;

	/**
	 * The base namespace for the result model
	 */
	private String baseNs;
	
	
	/**
	 * The rr namespace. Should not be changed.
	 */
	private final String rrNs = "http://www.w3.org/ns/r2rml#";

	/**
	 * @see MappingDocument
	 */
	MappingDocument mappingDocument = new MappingDocument();
	
	private String propertiesFilename;
	/**
	 * The properties, as read from the properties file.
	 */
	private Properties p = new Properties();
	
	private boolean verbose;
	
	private Database db;
	
	private Util util;
	
	public Parser(String propertiesFilename) {
		this.propertiesFilename = propertiesFilename;
	}
	
	public void parse() {

		init();
		
		try {
			//First find the logical table views
			ArrayList<LogicalTableView> logicalTableViews = findLogicalTableViews();
			mappingDocument.setLogicalTableViews(logicalTableViews);
			
			ArrayList<LogicalTableMapping> logicalTableMappings = findLogicalTableMappings();
			mappingDocument.setLogicalTableMappings(logicalTableMappings);
			
			for (int i = 0; i < mappingDocument.getLogicalTableMappings().size(); i++) {
				LogicalTableMapping logicalTableMapping = mappingDocument.getLogicalTableMappings().get(i);
			    logicalTableMapping.setSubjectMap(createSubjectMapForResource(mapModel.getResource(logicalTableMapping.getUri())));
			    logicalTableMapping.setPredicateObjectMaps(createPredicateObjectMapsForResource(mapModel.getResource(logicalTableMapping.getUri())));
			    mappingDocument.getLogicalTableMappings().set(i, logicalTableMapping);
			}
						
			createTriples();
			
			//resultModel.write(System.out, "TURTLE");
			
			//sparql("SELECT ?s ?p ?o FROM <" + baseNs + "> WHERE { ?s ?p ?o }", resultModel);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void createTriples() {
		for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
			ArrayList<Statement> triples = new ArrayList<Statement>();
		    try {
				SelectQuery selectQuery = logicalTableMapping.getView().getQuery();
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
											o = resultModel.createLiteral(value);
											if (verbose) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
										}
									} else {
										String value = util.fillTemplate(predicateObjectMap.getObjectTemplate(), rs);
										if (value != null) {
											o = resultModel.createLiteral(value, predicateObjectMap.getLanguage());
											if (verbose) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"@" + o.getLanguage());
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
								Literal o = resultModel.createLiteral(test == null? "" : test);
	
								if (verbose) log.info("Adding triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
								Statement st = resultModel.createStatement(s, p, o);
								resultModel.add(st);
								triples.add(st);
	
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
	
	public SubjectMap createSubjectMapForResource(Resource r) {
		log.info("Processing subject map for: <" + r.getURI() + ">");
		SubjectMap subjectMap = new SubjectMap();
		
		NodeIterator iter = mapModel.listObjectsOfProperty(r, mapModel.getProperty(rrNs + "subjectMap"));
	    while (iter.hasNext()) { //should be only 1
	    	RDFNode rn = iter.next();
	    	NodeIterator iterTemplate = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "template"));
		    while (iterTemplate.hasNext()) {
		    	RDFNode rnTemplate = iterTemplate.next();
		    	
		    	if (rnTemplate.isLiteral()) {
			    	log.info("Processing literal subject template: " + rnTemplate.asLiteral().toString());
			    	
			    	//Verify that it is indeed a literal and not some other type. Property rr:termType can have one of the following
			    	//values: rr:IRI, rr:BlankNode or rr:Literal
			    	NodeIterator iterTermType = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "termType"));
			    	if (iterTermType.hasNext()) {
				    	while (iterTermType.hasNext()) {
				    		RDFNode rnTermType = iterTermType.next();
				    		if (rnTermType.isResource() && rnTermType.asResource().getNameSpace().equals(rrNs)) {
				    			String termType = rnTermType.asResource().getLocalName();
				    			log.info("found rr:termType " + termType);
				    			if ("IRI".equals(termType)) {
				    				Template template = new Template(rnTemplate.asLiteral().toString(), TermType.IRI, baseNs, resultModel);
							    	subjectMap.setTemplate(template);
				    			} else if ("BlankNode".equals(termType)) {
				    				Template template = new Template(rnTemplate.asLiteral().toString(), TermType.BLANKNODE, baseNs, resultModel);
							    	subjectMap.setTemplate(template);
				    			} else if ("Literal".equals(termType)) {
				    				Template template = new Template(rnTemplate.asLiteral().toString(), TermType.LITERAL, baseNs, resultModel);
							    	subjectMap.setTemplate(template);
				    			} else {
				    				log.error("Unknown term type: " + termType + ". Terminating.");
				    				System.exit(0);
				    			}
				    		}
				    	}
		    		} else {
		    			Template template = new Template(rnTemplate.asLiteral().toString(), TermType.LITERAL, baseNs, resultModel);
				    	subjectMap.setTemplate(template);
		    		}
	    		} else {
	    			log.info("Processing node subject template: " + rnTemplate.asNode().toString());
			    	Template template = new Template(rnTemplate.asNode().toString(), TermType.IRI, baseNs, resultModel);
			    	subjectMap.setTemplate(template);
	    		}
		    	
		    	log.info("Logical table mapping uri is " + r.getURI());
		    	LogicalTableMapping ltm = mappingDocument.findLogicalTableMappingByUri(r.getURI());
		    	LogicalTableView ltv = ltm.getView(); 
		    	SelectQuery sq = ltv.getQuery();
		    	subjectMap.setSelectQuery(sq);
		    }
		    
		    NodeIterator iterClass = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "class"));
		    while (iterClass.hasNext()) { //should be only 1
		    	RDFNode rnClass = iterClass.next();
		    	if (verbose) log.info("Subject class is: " + rnClass.asResource().getURI());
		    	subjectMap.setClassUri(rnClass.asResource().getURI());
		    }
	    }
	    
	    return subjectMap;
	}
	
	public ArrayList<PredicateObjectMap> createPredicateObjectMapsForResource(Resource r) {
		log.info("Processing predicate object maps for: <" + r.getURI() + ">");
		ArrayList<PredicateObjectMap> predicateObjectMaps = new ArrayList<PredicateObjectMap>();
	    NodeIterator iterPredicateObject = mapModel.listObjectsOfProperty(r, mapModel.getProperty(rrNs + "predicateObjectMap"));
	    while (iterPredicateObject.hasNext()) {
	    	RDFNode rnPredicateObject = iterPredicateObject.next();
	    	
	    	PredicateObjectMap predicateObjectMap = new PredicateObjectMap();
	    	
	    	NodeIterator iterPredicate = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "predicate"));
	    	while (iterPredicate.hasNext()) { //should return only 1
		    	RDFNode rnPredicate = iterPredicate.next();
		    	if (verbose) log.info("Predicate is: " + rnPredicate.asResource().getURI());
		    	predicateObjectMap.setPredicate(rnPredicate.asResource().getURI());
		    }
	    	
	    	NodeIterator iterObject1 = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "object"));
	    	while (iterObject1.hasNext()) {
	    		log.info("Found object: " + iterObject1.toString());
	    		RDFNode rnTemplate = iterObject1.next();
	    		if (rnTemplate.isLiteral()) {
			    	log.info("Adding object map constant: " + rnTemplate.asLiteral().toString() + ". Treating it as a template with no fields.");
			    	Template template = new Template(rnTemplate.asLiteral().toString(), TermType.LITERAL, baseNs, resultModel);
			    	predicateObjectMap.setObjectTemplate(template);
	    		} else {
	    			log.info("Adding object map constant: " + rnTemplate.asNode().toString() + ". Treating it as a template with no fields.");
			    	Template template = new Template(rnTemplate.asNode().toString(), TermType.IRI, baseNs, resultModel);
			    	predicateObjectMap.setObjectTemplate(template);
	    		}
	    	}
	    	
	    	NodeIterator iterObjectMap2 = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "objectMap"));
	    	while (iterObjectMap2.hasNext()) {
		    	RDFNode rnObjectMap = iterObjectMap2.next();
		    	
		    	//Must have here an rr:column, an rr:template, or an rr:constant
		    	
		    	NodeIterator iterTemplate = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "template"));
			    while (iterTemplate.hasNext()) { //should return only 1
			    	RDFNode rnTemplate = iterTemplate.next();
			    	
			    	if (rnTemplate.isLiteral()) {
				    	log.info("Processing object map template: " + rnTemplate.asLiteral().toString() + ". Treating it as a template with no fields.");
				    	Template template = new Template(rnTemplate.asLiteral().toString(), TermType.LITERAL, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		} else {
		    			log.info("Processing object map template: " + rnTemplate.asNode().toString() + ". Treating it as a template with no fields.");
				    	Template template = new Template(rnTemplate.asNode().toString(), TermType.IRI, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		}
			    	//predicateObjectMap.setObjectColumn(predicateObjectMap.getObjectTemplate().getFields().get(0));
			    	//System.out.println("added objectQuery " + "SELECT " + predicateObjectMap.getObjectTemplate().getFields().get(0) + " FROM " + mappingDocument.findLogicalTableMappingByUri(r.getURI()).getView().getQuery().getTables().get(0).getName());
			    	//System.out.println("added objectTemplate " + rnTemplate.asLiteral().toString());
			    }
			    
		    	NodeIterator iterColumn = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "column"));
			    while (iterColumn.hasNext()) {
			    	RDFNode rnTemplate = iterColumn.next();
			    	String tempField = rnTemplate.asLiteral().toString();
			    	
			    	//objectFields.add(tempField);
			    	predicateObjectMap.setObjectColumn(tempField);
			    	log.info("Added object column: " + tempField);
			    }
			    
			    NodeIterator iterLanguage = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "language"));
			    while (iterLanguage.hasNext()) {
			    	RDFNode rnTemplate = iterLanguage.next();
			    	String language = rnTemplate.asLiteral().toString();
			    	
			    	predicateObjectMap.setLanguage(language);
			    	log.info("Added language: " + language);
			    }
			    
			    NodeIterator iterConstant = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "constant"));
			    while (iterConstant.hasNext()) {
			    	RDFNode rnTemplate = iterConstant.next();
			    	if (rnTemplate.isLiteral()) {
				    	log.info("Adding object map constant: " + rnTemplate.asLiteral().toString() + ". Treating it as a template with no fields.");
				    	Template template = new Template(rnTemplate.asLiteral().toString(), TermType.LITERAL, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		} else {
		    			log.info("Adding object map constant: " + rnTemplate.asNode().toString() + ". Treating it as a template with no fields.");
				    	Template template = new Template(rnTemplate.asNode().toString(), TermType.IRI, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		}
			    }
		    }
	    	predicateObjectMaps.add(predicateObjectMap);
	    }
	    
	    return predicateObjectMaps;
	}
	
	public ArrayList<LogicalTableMapping> findLogicalTableMappings() {
		ArrayList<LogicalTableMapping> results = new ArrayList<LogicalTableMapping>();
		Property logicalTable = mapModel.getProperty(rrNs + "logicalTable");
		ResIterator iter1 = mapModel.listSubjectsWithProperty(logicalTable);
		while (iter1.hasNext()) {
			Resource r = iter1.nextResource();
			log.info("Found logical table: <" + r.getURI() + ">");
			NodeIterator iter1b = mapModel.listObjectsOfProperty(r, logicalTable);
			while (iter1b.hasNext()) { //should be only 1
		    	RDFNode rn = iter1b.next();
		    	if (r.getURI() != null) {
			    	LogicalTableMapping logicalTableMapping = new LogicalTableMapping();
			    	logicalTableMapping.setUri(r.getURI());
			    	log.info("Looking up logical table view " + rn.asResource().getURI());
			    	if (rn.asResource().getURI() != null) {
			    		logicalTableMapping.setView(mappingDocument.findLogicalTableViewByUri(rn.asResource().getURI()));
			    	
			    		if (!contains(results, logicalTableMapping.getUri()))
			    			results.add(logicalTableMapping);
			    	}
			    	log.info("Added logical table mapping from uri <" + r.getURI() + ">");
		    	} else {
		    		log.info("Did not add logical table mapping from NULL uri");
		    	}
		    }
		}
		
		Property tableName = mapModel.getProperty(rrNs + "tableName");
		ResIterator iter2 = mapModel.listSubjectsWithProperty(tableName);
		while (iter2.hasNext()) {
		    Resource r = iter2.nextResource();
		    if (r.isLiteral()) {
		    	log.info("Found literal with a table name: <" + r.asLiteral().toString() + ">");
		    } else {
		    	log.info("Found resource with a table name: <" + r.getURI() + ">");
		    }
		    
		    NodeIterator iter2b = mapModel.listObjectsOfProperty(r, tableName);
		    while (iter2b.hasNext()) { //should be only 1
		    	RDFNode rn = iter2b.next();
		    	LogicalTableMapping logicalTableMapping = new LogicalTableMapping();

	    		LogicalTableView logicalTableView = new LogicalTableView();
	    		String newTable = rn.asLiteral().toString();
	    		newTable = util.stripQuotes(newTable);
	    		log.info("Found table name: " + newTable);
	    		SelectQuery sq = new SelectQuery(createQueryForTable(newTable), p);
	    		log.info("Setting SQL query for table " + newTable + ": " + sq.getQuery());
	    		logicalTableView.setQuery(sq);
	    		if (r.getURI() == null) {
			    	//figure out to which TriplesMap this rr:tableName belongs
			    	log.info("Found rr:tableName without parent.");
			    	LocalResultSet sparqlResults = util.sparql(mapModel, "SELECT ?x WHERE { ?x rr:logicalTable ?z . ?z rr:tableName \"\\\"" + newTable + "\\\"\" . } ");
			    	
			    	String triplesMapUri = sparqlResults.getRows().get(0).getResources().get(0).getUri();
			    	if (triplesMapUri != null) {
			    		logicalTableMapping.setUri(triplesMapUri);
			    	} else {
			    		log.error("Could not find triples map.");
			    	}
			    } else {
			    	logicalTableMapping.setUri(r.getURI());
			    }
		    	logicalTableMapping.setView(logicalTableView);

	    		results.add(logicalTableMapping);
	    		
		    }
		    
		}

		for (LogicalTableMapping l : results) {
			if (l.getView() == null ) {
				results.remove(l);
			}
		}
    	
		return results;
	}
	
	public ArrayList<LogicalTableView> findLogicalTableViews() {
		ArrayList<LogicalTableView> results = new ArrayList<LogicalTableView>();
		Property sqlQuery = mapModel.getProperty(rrNs + "sqlQuery");
		ResIterator iter = mapModel.listSubjectsWithProperty(sqlQuery);
		while (iter.hasNext()) {
		    Resource r = iter.nextResource();
		    NodeIterator iter2 = mapModel.listObjectsOfProperty(r, sqlQuery);
		    while (iter2.hasNext()) { //should only have one
		    	RDFNode rn = iter2.next();
		    	LogicalTableView logicalTableView = new LogicalTableView();
		    	logicalTableView.setUri(r.getURI());
		    	String query = rn.asLiteral().toString();
		    	//Windows standard line separator is "\r\n"--a carriage-return followed by a linefeed. The regex below matches any number of either of the two characters
		    	query = query.replaceAll("[\r\n]+", " ");
		    	
		    	log.info("Found query: <" + r.getURI() + "> with value: " + query);
		    	//Testing. Add a LIMIT 10 to avoid large datasets.
			    try {
					java.sql.ResultSet rs = db.query(query + " LIMIT 10");
					rs.close();
					log.info("Query tested ok.");
				} catch (SQLException e) {
					e.printStackTrace();
				}
		    	
		    	SelectQuery selectQuery = new SelectQuery(query, p);
		    	logicalTableView.setQuery(selectQuery);
		    	results.add(logicalTableView);
		    }
		}
		return results;
	}
	
	public String createQueryForTable(String tableName) {
		String result = "SELECT ";
		try {
			ArrayList<String> fields = new ArrayList<String>();
			
			java.sql.ResultSet rs;
			if (p.getProperty("db.driver").contains("mysql")) {
				rs = db.query("DESCRIBE " + tableName);
			} else {
				//postgres
				rs = db.query("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" + tableName + "'"); 
			}
			
			rs.beforeFirst();
			while (rs.next()) {
				//mysql: fields.add(rs.getString("Field"));
				fields.add("\"" + rs.getString(1) + "\"");
			}
			for (String f : fields) {
				result += f + ", ";
			}
			result = result.substring(0, result.lastIndexOf(','));
			rs.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		result += " FROM " + "\"" + tableName + "\"";
		log.info("result is: " + result);
		return result;
	}

	public void init() {
		try {
			p.load(new FileInputStream(propertiesFilename));
			
			//test source database
			db.openConnection();
					
			//test destination database
			if (p.getProperty("jena.storeOutputModelInDatabase").contains("true")) {
				db.openJenaConnection();
			}
			
			this.baseNs = p.getProperty("default.namespace");
			String mappingFilename = p.getProperty("mapping.file");
			
			InputStream isMap = FileManager.get().open(mappingFilename);
			mapModel = ModelFactory.createDefaultModel();
			mapModel.read(isMap, baseNs, p.getProperty("mapping.file.type"));
			//mapModel.write(System.out, properties.getProperty("mapping.file.type"));
			
			String inputModelFileName = p.getProperty("input.model");
			InputStream isRes = FileManager.get().open(inputModelFileName);
			
			Model resultBaseModel = ModelFactory.createDefaultModel();
			resultBaseModel.read(isRes, baseNs, p.getProperty("input.model.type"));
			//resultBaseModel.write(System.out, properties.getProperty("input.model.type"));
			
			String storeInDatabase = p.getProperty("jena.storeOutputModelInDatabase");
			String cleanDbOnStartup = p.getProperty("jena.cleanDbOnStartup");
			
			verbose =  p.containsKey("default.verbose") && (p.getProperty("default.verbose").contains("true") || p.getProperty("default.verbose").contains("yes"));
				
			log.info("Initialising");
			
			if (Boolean.valueOf(storeInDatabase)) {
				if (Boolean.valueOf(cleanDbOnStartup)) {
				    //Model model = SDBFactory.connectDefaultModel(db.jenaStore());
				    
				    //if (model.size() > 0) {
				    //	model.removeAll();
				    //}
					//db.getStore() .getJenaConnection().cleanDB();
				}
				
				Store store = db.jenaStore();
			    Model resultDbModel = SDBFactory.connectDefaultModel(store);
			    
			    resultDbModel.read(isRes, baseNs, p.getProperty("input.model.type"));
			    log.info("Store size is " + store.getSize());
//			    if (store.getSize() > 0) {
//				    StmtIterator sIter = resultDbModel.listStatements();
//				    for ( ; sIter.hasNext() ; ) {
//				        Statement stmt = sIter.nextStatement() ;
//				        log.info("stmt " + stmt) ;
//				    }
//				    sIter.close() ;
//			    }
			    
			    //Model resultDbModel = ModelFactory.createModelRDBMaker(db.getJenaConnection()).createModel("fresh");
				resultDbModel.add(resultBaseModel);
				
				//resultModel = ModelFactory.createInfModel(ReasonerRegistry.getRDFSReasoner(), resultDbModel);
				resultModel = ModelFactory.createDefaultModel();

				Map<String, String> prefixes = mapModel.getNsPrefixMap();
				log.info("Copy " + prefixes.size() + " prefixes from map model to persistent.");
				for (String s : mapModel.getNsPrefixMap().keySet()) {
					log.info(s + ": " + mapModel.getNsPrefixMap().get(s));
				}
				resultModel.setNsPrefixes(prefixes);
				
			} else {
				//resultModel = ModelFactory.createInfModel(ReasonerRegistry.getRDFSReasoner(), resultBaseModel);
				resultModel = ModelFactory.createDefaultModel();
				resultModel.setNsPrefixes(mapModel.getNsPrefixMap());
			}
						
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		Calendar c0 = Calendar.getInstance();
        long t0 = c0.getTimeInMillis();
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("app-context.xml");
		
		Parser parser = (Parser) context.getBean("parser");
		parser.parse();
		
		Calendar c1 = Calendar.getInstance();
        long t1 = c1.getTimeInMillis();
        log.info("Finished in " + (t1 - t0) + " milliseconds.");
	}
	
	public String getPropertiesFilename() {
		return propertiesFilename;
	}
	
	boolean contains(ArrayList<LogicalTableMapping> logicalTableMappings, String uri) {
		for (LogicalTableMapping logicalTableMapping : logicalTableMappings) {
			if (logicalTableMapping.getUri().equals(uri)) return true;
		}
		return false;
	}
	
	public void setPropertiesFilename(String propertiesFilename) {
		this.propertiesFilename = propertiesFilename;
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
	
}
