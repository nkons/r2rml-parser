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
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.InfModel;
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
import com.hp.hpl.jena.reasoner.ReasonerRegistry;
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
	private InfModel resultModel;

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
		
		//test source database
		db.openConnection();
				
		//test destination database
		if (p.getProperty("jena.storeOutputModelInDatabase").contains("true")) {
			db.openJenaConnection();
		}
		
		
		
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
				
				java.sql.ResultSet rs = db.query(selectQuery.getQuery());
				rs.beforeFirst();
				int tripleCount = 0;
				while (rs.next()) {

					String resultSubject = util.fillTemplate(logicalTableMapping.getSubjectMap().getTemplate(), rs);
					
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
							if (!util.isUriTemplate(resultModel, predicateObjectMap.getObjectTemplate())) {
								Literal o = resultModel.createLiteral(util.fillTemplate(predicateObjectMap.getObjectTemplate(), rs));
								
								if (this.p.containsKey("default.verbose")) log.info("Adding literal triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.getString() + ">");
								Statement st = resultModel.createStatement(s, p, o);
								resultModel.add(st);
								triples.add(st);
							} else {
								RDFNode o = resultModel.createResource(util.fillTemplate(predicateObjectMap.getObjectTemplate(), rs));
								
								if (this.p.containsKey("default.verbose")) log.info("Adding resource triple: <" + s.getURI() + ">, <" + p.getURI() + ">, <" + o.asResource().getURI() + ">");
								Statement st = resultModel.createStatement(s, p, o);
								resultModel.add(st);
								triples.add(st);
								
							}
						} else if (predicateObjectMap.getObjectColumn() != null) {
							String test = rs.getString(predicateObjectMap.getObjectColumn());
							Literal o = resultModel.createLiteral(test == null? "" : test);

							if (verbose) log.info("Adding triple: <" + s.getURI() + ">, <" + p.getURI() + ">, \"" + o.getString() + "\"");
							Statement st = resultModel.createStatement(s, p, o);
							resultModel.add(st);
							triples.add(st);

						} 
					}

					tripleCount++;
					if (tripleCount % 100 == 0) log.info("at " + tripleCount + " triples");
				}
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		    logicalTableMapping.setTriples(triples);
		    log.info("Generated " + triples.size() + " statements from table mapping <" + logicalTableMapping.getUri() + ">");
	    }
		
		if (p.getProperty("jena.storeOutputModelInDatabase").contains("false")) {
			log.info("Writing model to file. Model has " + resultModel.listStatements().toList().size() + " statements.");
			try {
				resultModel.write(new FileOutputStream(p.getProperty("jena.destinationFileName")), p.getProperty("jena.destinationFileSyntax"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		//log the results
		try {
			File logFile = new File(p.getProperty("default.log"));
			log.info("Writing results to " + logFile.getAbsolutePath());
			FileOutputStream fop = new FileOutputStream(logFile);
			String header = "Mapping URI\tTriples Size\tTimestamp\n";
			fop.write(header.getBytes());
			//run again on the table mappings
			for (LogicalTableMapping logicalTableMapping : mappingDocument.getLogicalTableMappings()) {
				String line = logicalTableMapping.getUri() + "\t" + logicalTableMapping.getTriples().size() + "\t" + new Date() + "\n";
				fop.write(line.getBytes());
			}
			fop.flush();
			fop.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
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
		    	log.info("Processing subject template: " + rnTemplate.asLiteral().toString());
		    	Template template = new Template(rnTemplate.asLiteral().toString());
		    	subjectMap.setTemplate(template);
			    subjectMap.setSelectQuery(mappingDocument.findLogicalTableMappingByUri(r.getURI()).getView().getQuery());
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
	    	
	    	NodeIterator iterObjectMap = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "objectMap"));
	    	while (iterObjectMap.hasNext()) {
		    	RDFNode rnObjectMap = iterObjectMap.next();
		    	
		    	//Must have here either an rr:column or an rr:template
		    	
		    	NodeIterator iterTemplate = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "template"));
			    while (iterTemplate.hasNext()) { //should return only 1
			    	RDFNode rnTemplate = iterTemplate.next();
			    	log.info("Processing object map template: " + rnTemplate.asLiteral().toString());
			    	Template template = new Template(rnTemplate.asLiteral().toString());
			    	
			    	predicateObjectMap.setObjectTemplate(template);
			    	//predicateObjectMap.setObjectColumn(predicateObjectMap.getObjectTemplate().getFields().get(0));
			    	//System.out.println("added objectQuery " + "SELECT " + predicateObjectMap.getObjectTemplate().getFields().get(0) + " FROM " + mappingDocument.findLogicalTableMappingByUri(r.getURI()).getView().getQuery().getTables().get(0).getName());
			    	//System.out.println("added objectTemplate " + rnTemplate.asLiteral().toString());
			    }
			    
		    	NodeIterator iterTemplate2 = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "column"));
			    while (iterTemplate2.hasNext()) {
			    	RDFNode rnTemplate = iterTemplate2.next();
			    	String tempField = rnTemplate.asLiteral().toString();
			    	
			    	//objectFields.add(tempField);
			    	predicateObjectMap.setObjectColumn(tempField);
			    	log.info("Added object column: " + tempField);
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
		    	LogicalTableMapping logicalTableMapping = new LogicalTableMapping();
		    	logicalTableMapping.setUri(r.getURI());
		    	logicalTableMapping.setView(mappingDocument.findLogicalTableViewByUri(rn.asResource().getURI()));
		    	results.add(logicalTableMapping);
		    }
		}
		
		Property tableName = mapModel.getProperty(rrNs + "tableName");
		ResIterator iter2 = mapModel.listSubjectsWithProperty(tableName);
		while (iter2.hasNext()) {
		    Resource r = iter2.nextResource();
	    	log.info("Found table name: <" + r.getURI() + ">");
		    NodeIterator iter2b = mapModel.listObjectsOfProperty(r, tableName);
		    while (iter2b.hasNext()) { //should be only 1
		    	RDFNode rn = iter2b.next();
		    	LogicalTableMapping logicalTableMapping = new LogicalTableMapping();
		    	logicalTableMapping.setUri(r.getURI());
		    		LogicalTableView logicalTableView = new LogicalTableView();
		    		String newTable = rn.asLiteral().toString();
		    		logicalTableView.setQuery(new SelectQuery(createQueryForTable(newTable), p));
		    	logicalTableMapping.setView(logicalTableView);
		    	results.add(logicalTableMapping);
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

	public void sparql(String query, Model model) {

		Query q = QueryFactory.create(query);
		QueryExecution qexec = QueryExecutionFactory.create(q, model);
		ResultSet results = qexec.execSelect();

		try {
			ResultSetFormatter.out(System.out, results, q);
		} finally {
			qexec.close();
		}
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
				fields.add(rs.getString(1));
			}
			for (String f : fields) {
				result += f + ", ";
			}
			result = result.substring(0, result.lastIndexOf(','));
			rs.close();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		result += " FROM " + tableName;
		log.info("result is: " + result);
		return result;
	}

	public void init() {
		try {
			p.load(new FileInputStream(propertiesFilename));
			this.baseNs = p.getProperty("default.namespace");
			String mappingFilename = p.getProperty("mapping.file");
			
			InputStream isMap = FileManager.get().open(mappingFilename);
			mapModel = ModelFactory.createDefaultModel();
			mapModel.read(isMap, baseNs, p.getProperty("mapping.file.type"));
			//mapModel.write(System.out, properties.getProperty("mapping.file.type"));
			
			String resultModelFileName = p.getProperty("input.model");
			InputStream isRes = FileManager.get().open(resultModelFileName);
			
			Model resultBaseModel = ModelFactory.createDefaultModel();
			resultBaseModel.read(isRes, baseNs, p.getProperty("input.model.type"));
			//resultBaseModel.write(System.out, properties.getProperty("input.model.type"));
			
			String storeInDatabase = p.getProperty("jena.storeOutputModelInDatabase");
			String cleanDbOnStartup = p.getProperty("jena.cleanDbOnStartup");
			
			verbose = p.getProperty("default.verbose").contains("true");
				
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
			    log.info("store size is " + store.getSize());
			    if (store.getSize() > 0) {
				    StmtIterator sIter = resultDbModel.listStatements() ;
				    for ( ; sIter.hasNext() ; )
				    {
				        Statement stmt = sIter.nextStatement() ;
				        log.info("stmt " + stmt) ;
				    }
				    sIter.close() ;
				    //store.close() ;
			    }
			    
			    //Model resultDbModel = ModelFactory.createModelRDBMaker(db.getJenaConnection()).createModel("fresh");
				resultDbModel.add(resultBaseModel);
				
				resultModel = ModelFactory.createInfModel(ReasonerRegistry.getRDFSReasoner(), resultDbModel);

				Map<String, String> prefixes = mapModel.getNsPrefixMap();
				log.info("Copy " + prefixes.size() + " prefixes from map model to persistent.");
				for (String s : mapModel.getNsPrefixMap().keySet()) {
					log.info(s + ": " + mapModel.getNsPrefixMap().get(s));
				}
				resultModel.setNsPrefixes(prefixes);
				
			} else {
				resultModel = ModelFactory.createInfModel(ReasonerRegistry.getRDFSReasoner(), resultBaseModel);
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
