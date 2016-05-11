/**
 * Licensed under the Creative Commons Attribution-NonCommercial 4.0 Unported 
 * License (the "License"). You may not use this file except in compliance with
 * the License. You may obtain a copy of the License at:
 * 
 *  http://creativecommons.org/licenses/by-nc/4.0/
 *  
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */
package gr.seab.r2rml.beans;

import gr.seab.r2rml.beans.util.LogicalTableMappingComparator;
import gr.seab.r2rml.entities.DatabaseType;
import gr.seab.r2rml.entities.LogicalTableMapping;
import gr.seab.r2rml.entities.LogicalTableView;
import gr.seab.r2rml.entities.MappingDocument;
import gr.seab.r2rml.entities.PredicateObjectMap;
import gr.seab.r2rml.entities.RefObjectMap;
import gr.seab.r2rml.entities.SubjectMap;
import gr.seab.r2rml.entities.Template;
import gr.seab.r2rml.entities.TermType;
import gr.seab.r2rml.entities.sparql.LocalResultSet;
import gr.seab.r2rml.entities.sql.SelectQuery;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.BaseDatatype;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.ReadWrite;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.NodeIterator;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;

/**
 * Parses a valid R2RML file and produces a mapping document
 * @see MappingDocument
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
	 * The xsd namespace. Should not be changed.
	 */	
	private final String xsdNs = "http://www.w3.org/2001/XMLSchema#";

	/**
	 * @see MappingDocument
	 */
	MappingDocument mappingDocument = new MappingDocument();
	
	private String propertiesFilename;
	/**
	 * The properties, as read from the properties file.
	 */
	private Properties properties;
	
	private boolean verbose;
	
	private Database db;
	
	private Util util;
	
	public Parser() {
	}
	
	@SuppressWarnings("unchecked")
	public MappingDocument parse() {

		init();
		
		log.info("Initialized.");
		
		try {
            DatabaseType databaseType = util.findDatabaseType(properties.getProperty("db.driver"));
            mappingDocument.setDatabaseType(databaseType);
            //First find the logical table views
			LinkedList<LogicalTableView> logicalTableViews = findLogicalTableViews();
			mappingDocument.setLogicalTableViews(logicalTableViews);
			log.info("Mapping document has " + logicalTableViews.size() + " logical table views.");
			
			LinkedList<LogicalTableMapping> logicalTableMappings = findLogicalTableMappings();
			mappingDocument.setLogicalTableMappings(logicalTableMappings);
			log.info("Mapping document has " + logicalTableMappings.size() + " logical table mappings.");
			
			for (int i = 0; i < mappingDocument.getLogicalTableMappings().size(); i++) {
				LogicalTableMapping logicalTableMapping = mappingDocument.getLogicalTableMappings().get(i);
			    logicalTableMapping.setSubjectMap(createSubjectMapForResource(mapModel.getResource(logicalTableMapping.getUri())));
			    logicalTableMapping.setPredicateObjectMaps(createPredicateObjectMapsForResource(mapModel.getResource(logicalTableMapping.getUri())));
			    mappingDocument.getLogicalTableMappings().set(i, logicalTableMapping);
			}
			
			//Sorting: evaluate first the logical table mappings without reference to a parent triples map
			@SuppressWarnings("rawtypes")
			Comparator c =  new LogicalTableMappingComparator();
			Collections.sort(mappingDocument.getLogicalTableMappings(), c);
			
			if (verbose) {
				log.info("Logical table mappings will be parsed in the following order:");
				for (LogicalTableMapping ltm : mappingDocument.getLogicalTableMappings()) {
					log.info(" Table mapping uri: " + ltm.getUri());
				}
			}
			//resultModel.write(System.out, "TURTLE");
			
			//sparql("SELECT ?s ?p ?o FROM <" + baseNs + "> WHERE { ?s ?p ?o }", resultModel);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return mappingDocument;
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
			    	log.info("Processing literal subject template: " + rnTemplate.asLiteral().getString());
			    	
			    	//Verify that it is indeed a literal and not some other type. Property rr:termType can have one of the following
			    	//values: rr:IRI, rr:BlankNode or rr:Literal
			    	NodeIterator iterTermType = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "termType"));
			    	if (iterTermType.hasNext()) {
				    	while (iterTermType.hasNext()) {
				    		RDFNode rnTermType = iterTermType.next();
				    		if (rnTermType.isResource() && rnTermType.asResource().getNameSpace().equals(rrNs)) {
				    			String termType = rnTermType.asResource().getLocalName();
				    			log.info("Found rr:termType " + termType);
				    			if ("IRI".equals(termType)) {
				    				Template template = new Template(rnTemplate.asLiteral(), TermType.IRI, baseNs, resultModel);
							    	subjectMap.setTemplate(template);
				    			} else if ("BlankNode".equals(termType)) {
				    				Template template = new Template(rnTemplate.asLiteral(), TermType.BLANKNODE, baseNs, resultModel);
							    	subjectMap.setTemplate(template);
				    			} else if ("Literal".equals(termType)) {
				    				Template template = new Template(rnTemplate.asLiteral(), TermType.LITERAL, baseNs, resultModel);
							    	subjectMap.setTemplate(template);
				    			} else {
				    				log.error("Unknown term type: " + termType + ". Terminating.");
				    				System.exit(1);
				    			}
				    		}
				    	}
		    		} else {
		    			Template template = new Template(rnTemplate.asLiteral(), TermType.LITERAL, baseNs, resultModel);
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
		    	SelectQuery sq = ltv.getSelectQuery();
		    	subjectMap.setSelectQuery(sq);
		    }
		    
		    NodeIterator iterColumn = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "column"));
		    while (iterColumn.hasNext()) {
		    	RDFNode rnColumn = iterColumn.next();
		    	String tempColumn = rnColumn.asLiteral().getString();
		    	String templateText = "{" + tempColumn + "}";
		    	
		    	NodeIterator iterTermType = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "termType"));
		    	if (iterTermType.hasNext()) {
			    	while (iterTermType.hasNext()) {
			    		RDFNode rnTermType = iterTermType.next();
			    		if (rnTermType.isResource() && rnTermType.asResource().getNameSpace().equals(rrNs)) {
			    			String termType = rnTermType.asResource().getLocalName();
			    			if (verbose) log.info("Found rr:termType " + termType);
			    			if ("IRI".equals(termType)) {
			    				Template template = new Template(templateText, TermType.IRI, baseNs, resultModel);
						    	subjectMap.setTemplate(template);
			    			} else if ("BlankNode".equals(termType)) {
			    				Template template = new Template(templateText, TermType.BLANKNODE, baseNs, resultModel);
						    	subjectMap.setTemplate(template);
			    			} else if ("Literal".equals(termType)) {
			    				Template template = new Template(templateText, TermType.LITERAL, baseNs, resultModel);
						    	subjectMap.setTemplate(template);
			    			} else {
			    				log.error("Unknown term type: " + termType + ". Terminating.");
			    				System.exit(1);
			    			}
			    		}
			    	}
	    		} else {
	    			Template template = new Template(templateText, TermType.IRI, baseNs, resultModel);
			    	subjectMap.setTemplate(template);
	    		}
		    	log.info("Added subject template " + templateText + " from column " + tempColumn);
		    }
		    
		    NodeIterator iterClass = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "class"));
		    ArrayList<String> classUris = new ArrayList<String>();
		    while (iterClass.hasNext()) { //can be more than 1
		    	RDFNode rnClass = iterClass.next();
		    	if (verbose) log.info("Subject class is: " + rnClass.asResource().getURI());
		    	classUris.add(rnClass.asResource().getURI());
		    }
		    subjectMap.setClassUris(classUris);
		    
		    NodeIterator iterGraphMap = mapModel.listObjectsOfProperty(rn.asResource(), mapModel.getProperty(rrNs + "graphMap"));
		    while (iterGraphMap.hasNext()) { //should be only 1
		    	RDFNode rnGraphMap = iterGraphMap.next();
		    	log.info("triples belong to graphMap " + rnGraphMap.asResource().getURI());
		    	if (rnGraphMap.asResource().getURI() == null) {
		    		log.info("graphMap is either a template or a constant");
		    		//TODO GraphMap should be inserted in subject
		    	}
		    }
	    }
	    
	    NodeIterator iterSubject = mapModel.listObjectsOfProperty(r, mapModel.getProperty(rrNs + "subject"));
	    while (iterSubject.hasNext()) { //should be only 1
	    	RDFNode rnSubject = iterSubject.next();
	    	log.info("Found subject: " + rnSubject.toString());
	    	Template template = new Template(rnSubject.toString(), TermType.IRI, baseNs, resultModel);
	    	subjectMap.setTemplate(template);
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
	    	ArrayList<String> predicates = new ArrayList<String>();
	    	while (iterPredicate.hasNext()) { //can return more than 1
		    	RDFNode rnPredicate = iterPredicate.next();
		    	if (verbose) log.info("Predicate is: " + rnPredicate.asResource().getURI());
		    	predicates.add(rnPredicate.asResource().getURI());
		    }
	    	
	    	NodeIterator iterPredicateMap = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "predicateMap"));
	    	while (iterPredicateMap.hasNext()) { //can return more than 1
		    	RDFNode rnPredicateMap = iterPredicateMap.next();
		    	
		    	NodeIterator iterConstant = mapModel.listObjectsOfProperty(rnPredicateMap.asResource(), mapModel.getProperty(rrNs + "constant"));
			    while (iterConstant.hasNext()) {
			    	RDFNode rnConstant = iterConstant.next();

			    	if (rnConstant.isLiteral()) {
			    		log.info("Adding predicate map constant literal: " + rnConstant.asNode().toString() + ".");
		    			predicates.add(rnConstant.asLiteral().getString());
			    	} else {
		    			log.info("Adding predicate map constant uri: " + rnConstant.asNode().toString() + ".");
		    			predicates.add(rnConstant.asResource().getURI());
			    	}
			    }
		    }
	    	predicateObjectMap.setPredicates(predicates);
	    	
	    	NodeIterator iterObject1 = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "object"));
	    	while (iterObject1.hasNext()) {
	    		RDFNode rnObject = iterObject1.next();
	    		log.info("Found object: " + rnObject.toString());
	    		if (rnObject.isLiteral()) {
			    	log.info("Adding object map constant: " + rnObject.asLiteral().toString() + ". Treating it as a template with no fields.");
			    	Template template = new Template(rnObject.asLiteral(), TermType.LITERAL, baseNs, resultModel);
			    	predicateObjectMap.setObjectTemplate(template);
	    		} else {
	    			log.info("Adding object map constant: " + rnObject.asNode().toString() + ". Treating it as a template with no fields.");
			    	Template template = new Template(rnObject.asNode().toString(), TermType.IRI, baseNs, resultModel);
			    	predicateObjectMap.setObjectTemplate(template);
	    		}
	    	}
	    	
	    	NodeIterator iterObjectMap2 = mapModel.listObjectsOfProperty(rnPredicateObject.asResource(), mapModel.getProperty(rrNs + "objectMap"));
	    	while (iterObjectMap2.hasNext()) {
		    	RDFNode rnObjectMap = iterObjectMap2.next();
		    	
		    	//Must have here an rr:column, an rr:template, an rr:constant, or an rr:parentTriplesMap
		    	
		    	NodeIterator iterTemplate = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "template"));
			    while (iterTemplate.hasNext()) { //should return only 1
			    	RDFNode rnTemplate = iterTemplate.next();
			    	
			    	if (rnTemplate.isLiteral()) {
				    	log.info("Processing object map template: " + rnTemplate.asLiteral().getString());
				    	Template template = new Template(rnTemplate.asLiteral(), TermType.LITERAL, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		} else {
		    			log.info("Processing object map template: " + rnTemplate.asNode().toString());
				    	Template template = new Template(rnTemplate.asNode().toString(), TermType.IRI, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		}
			    	//predicateObjectMap.setObjectColumn(predicateObjectMap.getObjectTemplate().getFields().get(0));
			    	//System.out.println("added objectQuery " + "SELECT " + predicateObjectMap.getObjectTemplate().getFields().get(0) + " FROM " + mappingDocument.findLogicalTableMappingByUri(r.getURI()).getView().getQuery().getTables().get(0).getName());
			    	//System.out.println("added objectTemplate " + rnTemplate.asLiteral().toString());
			    }
			    
		    	NodeIterator iterColumn = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "column"));
			    while (iterColumn.hasNext()) {
			    	RDFNode rnColumn = iterColumn.next();
			    	String tempField = rnColumn.asLiteral().getString();
			    	
			    	//objectFields.add(tempField);
			    	String templateText = "{" + tempField + "}";
	    			Template template = new Template(templateText, TermType.AUTO, baseNs, resultModel);
			    	predicateObjectMap.setObjectTemplate(template);
			    	
			    	predicateObjectMap.setObjectColumn(tempField);
			    	log.info("Added object column and template from column: " + tempField);
			    }
			    
			    NodeIterator iterLanguage = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "language"));
			    while (iterLanguage.hasNext()) {
			    	RDFNode rnLanguage = iterLanguage.next();
			    	String language = rnLanguage.asLiteral().getString();
			    	
			    	predicateObjectMap.getObjectTemplate().setLanguage(language);
			    	log.info("Added language: " + language);
			    }
			    
			    NodeIterator iterDataType = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "datatype"));
			    while (iterDataType.hasNext()) {
			    	RDFNode rnDataType = iterDataType.next();
			    	
			    	if (xsdNs.equals(rnDataType.asResource().getNameSpace())) {
				    	String dataType = rnDataType.asResource().getLocalName();
				    	log.info("Found datatype xsd:" + dataType);
				    	BaseDatatype baseDataType = util.findDataType(dataType);
				    	predicateObjectMap.setDataType(baseDataType);
			    	} else {
			    		predicateObjectMap.setDataType(new BaseDatatype(rnDataType.asResource().getURI()));
			    	}
			    }
			    
			    NodeIterator iterConstant = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "constant"));
			    while (iterConstant.hasNext()) {
			    	RDFNode rnConstant = iterConstant.next();
			    	if (rnConstant.isLiteral()) {
				    	log.info("Adding object map constant literal: " + rnConstant.asLiteral().getString());
				    	Template template = new Template(rnConstant.asLiteral(), TermType.LITERAL, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		} else {
		    			log.info("Adding object map constant iri: " + rnConstant.asNode().toString());
				    	Template template = new Template(rnConstant.asNode().toString(), TermType.IRI, baseNs, resultModel);
				    	predicateObjectMap.setObjectTemplate(template);
		    		}
			    }
			    
			    //If there is a type declared, add it the object template
			    NodeIterator iterTermType = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "termType"));
		    	if (iterTermType.hasNext()) {
		    		Template template = predicateObjectMap.getObjectTemplate();
			    	while (iterTermType.hasNext()) { //should only be 1
			    		RDFNode rnTermType = iterTermType.next();
			    		if (rnTermType.isResource() && rnTermType.asResource().getNameSpace().equals(rrNs)) {
			    			String termType = rnTermType.asResource().getLocalName();
			    			log.info("Found rr:termType " + termType);
			    			if ("IRI".equals(termType)) {
			    				template.setTermType(TermType.IRI);
			    			} else if ("BlankNode".equals(termType)) {
			    				template.setTermType(TermType.BLANKNODE);
			    			} else if ("Literal".equals(termType)) {
			    				template.setTermType(TermType.LITERAL);
			    			} else {
			    				log.error("Unknown term type: " + termType + ". Terminating.");
			    				System.exit(1);
			    			}
			    		}
			    	}
			    	predicateObjectMap.setObjectTemplate(template);
	    		}

		    	NodeIterator iterParentTriplesMap = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "parentTriplesMap"));
			    while (iterParentTriplesMap.hasNext()) {
			    	RDFNode rnParentTriplesMap = iterParentTriplesMap.next();
			    	log.info("Found rr:parentTriplesMap " + rnParentTriplesMap.asResource().getURI());
			    	RefObjectMap refObjectMap = new RefObjectMap();
			    	refObjectMap.setParentTriplesMapUri(rnParentTriplesMap.asResource().getURI());
			    	
			    	NodeIterator iterJoinCondition = mapModel.listObjectsOfProperty(rnObjectMap.asResource(), mapModel.getProperty(rrNs + "joinCondition"));
			    	while (iterJoinCondition.hasNext()) {
			    		RDFNode rnJoinCondition = iterJoinCondition.next();
			    		log.info("Found rr:joinCondition " + rnJoinCondition.asResource().getURI());
			    		NodeIterator iterChild = mapModel.listObjectsOfProperty(rnJoinCondition.asResource(), mapModel.getProperty(rrNs + "child"));
			    		while (iterChild.hasNext()) {
			    			RDFNode rnChild = iterChild.next();
			    			log.info("Found rr:child " + rnChild.asLiteral().getString());
			    			refObjectMap.setChild(rnChild.asLiteral().getString());
			    		}
			    		
			    		NodeIterator iterParent = mapModel.listObjectsOfProperty(rnJoinCondition.asResource(), mapModel.getProperty(rrNs + "parent"));
			    		while (iterParent.hasNext()) {
			    			RDFNode rnParent = iterParent.next();
			    			log.info("Found rr:parent " + rnParent.asLiteral().getString());
			    			refObjectMap.setParent(rnParent.asLiteral().getString());
			    		}
			    	}
			    	predicateObjectMap.setRefObjectMap(refObjectMap);
			    }
		    }
	    	predicateObjectMaps.add(predicateObjectMap);
	    }
	    
	    return predicateObjectMaps;
	}
	
	public LinkedList<LogicalTableMapping> findLogicalTableMappings() {
		LinkedList<LogicalTableMapping> results = new LinkedList<LogicalTableMapping>();
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
			    		LogicalTableView ltv = mappingDocument.findLogicalTableViewByUri(rn.asResource().getURI());
			    		logicalTableMapping.setView(ltv);
			    		
				    	if (!contains(results, logicalTableMapping.getUri()))
			    			results.add(logicalTableMapping);
			    	} else {
			    		//Then we can either have a rr:sqlQuery with the query or a rr:tableName with the table name
			    		LocalResultSet sparqlResults = util.sparql(mapModel, "SELECT ?z2 WHERE { <" + r.getURI() + "> rr:logicalTable ?z1 . ?z1 rr:sqlQuery ?z2 . } ");
			    		if (sparqlResults.getRows().size() > 0) {
				    		String sqlQuery = sparqlResults.getRows().get(0).getResources().get(0).getLocalName();
					    	if (sqlQuery != null) {
					    		sqlQuery = sqlQuery.replaceAll("[\r\n]+", " ");
						    	if (sqlQuery.indexOf(';') != -1) sqlQuery = sqlQuery.replace(';', ' ');
						    	
					    		SelectQuery test = new SelectQuery(sqlQuery, properties);
					    		LogicalTableView logicalTableView = mappingDocument.findLogicalTableViewByQuery(test.getQuery());
					    		logicalTableMapping.setView(logicalTableView);
					    		
						    	if (!contains(results, logicalTableMapping.getUri()))
					    			results.add(logicalTableMapping);
					    	} else {
					    		log.error("Could not find rr:sqlQuery.");
					    	}
			    		} else {
			    			sparqlResults = util.sparql(mapModel, "SELECT ?z2 WHERE { <" + r.getURI() + "> rr:logicalTable ?z1 . ?z1 rr:tableName ?z2 . } ");
			    			if (sparqlResults.getRows().size() > 0) {
			    				String tableName = sparqlResults.getRows().get(0).getResources().get(0).getLocalName();
			    				tableName = tableName.replaceAll("\"", "");
			    				log.info("Found tableName " + tableName);
			    				LogicalTableView logicalTableView = new LogicalTableView();
			    				SelectQuery sq = new SelectQuery(createQueryForTable(tableName), properties);
			    				logicalTableView.setSelectQuery(sq);
			    				logicalTableMapping.setView(logicalTableView);
			    				
			    				if (!contains(results, logicalTableMapping.getUri()))
					    			results.add(logicalTableMapping);
			    			}
			    		}
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
	    		String oldTable = rn.asLiteral().getString();
	    		String newTable = util.stripQuotes(oldTable);
	    		log.info("Found table name: " + newTable);
	    		SelectQuery sq = new SelectQuery(createQueryForTable(newTable), properties);
	    		log.info("Setting SQL query for table " + newTable + ": " + sq.getQuery());
	    		logicalTableView.setSelectQuery(sq);
	    		if (r.getURI() == null) {
			    	//figure out to which TriplesMap this rr:tableName belongs
			    	log.info("Found rr:tableName " + oldTable + " without parent.");
			    	//LocalResultSet sparqlResults = util.sparql(mapModel, "SELECT ?x WHERE { ?x rr:logicalTable ?z . ?z rr:tableName " + newTable + " . } ");
			    	if (util.findDatabaseType(properties.getProperty("db.driver")).equals("postgresql")) {
			    		newTable = "\"\\\"" + newTable + "\\\"\"";
			    	} else {
			    		newTable = "\"" + newTable + "\"";
			    	}
			    	LocalResultSet sparqlResults = util.sparql(mapModel, "SELECT ?x WHERE { ?x rr:logicalTable ?z . ?z rr:tableName '" + oldTable + "' . } ");

			    	if (sparqlResults.getRows().size() > 0) {
			    		String triplesMapUri = sparqlResults.getRows().get(0).getResources().get(0).getUri();
			    		logicalTableMapping.setUri(triplesMapUri);
			    		log.info("The rr:tableName " + oldTable + " is in triples map " + triplesMapUri);
			    	} else {
			    		log.error("Could not find triples map.");
			    	}
			    } else {
			    	logicalTableMapping.setUri(r.getURI());
			    }
		    	logicalTableMapping.setView(logicalTableView);

		    	if (!contains(results, logicalTableMapping.getUri()))
		    		results.add(logicalTableMapping);
		    }
		}

		//prevent java.util.ConcurrentModificationException
		for (Iterator<LogicalTableMapping> it = results.iterator(); it.hasNext(); ) {
			LogicalTableMapping logicalTableMapping = it.next();
			if (logicalTableMapping.getView() == null || logicalTableMapping.getUri() == null) {
				it.remove();
			}
		}
    	
		return results;
	}
	
	public LinkedList<LogicalTableView> findLogicalTableViews() {
		LinkedList<LogicalTableView> results = new LinkedList<LogicalTableView>();
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
		    	if (query.indexOf(';') != -1) query = query.replace(';', ' ');
		    	
		    	log.info("Testing query: <" + r.getURI() + "> with value: " + query);
		    	db.testQuery(query);
		    	
		    	SelectQuery selectQuery = new SelectQuery(query, properties);

		    	//if the dump is incremental, add an order by to ensure results are retrieved in the same order
		    	if (properties.containsKey("jena.jena.storeOutputModelUsingTdb")
		    		&& properties.getProperty("jena.jena.storeOutputModelUsingTdb").contains("false")
		    		&& properties.containsKey("default.incremental")
		    		&& properties.getProperty("default.incremental").contains("true")) {
		    		String q = selectQuery.getQuery();
		    		if (q.toLowerCase().indexOf("order by") == -1) {
		    			String fieldName = selectQuery.getFields().get(0).getName();
		    			int as = fieldName.toLowerCase().indexOf(" as ");
		    			if (as > -1) {
		    				fieldName = fieldName.substring(0, as);
			    			q += " order by " + fieldName;
		    				selectQuery.setQuery(q);
		    				log.info("Added ORDER BY to query that now is: " + q);
		    			}
		    		}
		    	}
		    	
		    	logicalTableView.setSelectQuery(selectQuery);
		    	results.add(logicalTableView);
		    }
		}
		return results;
	}
	
	public String createQueryForTable(String tableName) {
		
		String result = "SELECT ";
		ArrayList<String> fields = new ArrayList<String>();

		java.sql.Statement stmt = db.newStatement();

		try {
			java.sql.ResultSet rs;
			if (mappingDocument.getDatabaseType() == DatabaseType.POSTGRESQL) {
				rs = stmt.executeQuery("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '" + tableName + "'");
			} else if (mappingDocument.getDatabaseType() == DatabaseType.MYSQL) {
				rs = stmt.executeQuery("DESCRIBE " + tableName);
			} else if (mappingDocument.getDatabaseType() == DatabaseType.ORACLE) {
                rs = stmt.executeQuery("SELECT column_name FROM all_tab_cols WHERE table_name = '" + tableName + "'");
            } else {
                rs = null;
                log.error("Unknown database type. Terminating.");
                System.exit(1);
            }

			rs.beforeFirst();
			while (rs.next()) {
				//mysql: fields.add(rs.getString("Field"));
				if (mappingDocument.getDatabaseType() == DatabaseType.POSTGRESQL || mappingDocument.getDatabaseType() == DatabaseType.ORACLE) {
					fields.add("\"" + rs.getString(1) + "\"");
				} else {
					fields.add(rs.getString("Field"));
				}
			}
			for (String f : fields) {
				if (mappingDocument.getDatabaseType() == DatabaseType.POSTGRESQL || mappingDocument.getDatabaseType() == DatabaseType.ORACLE) {
					result += "\"" + tableName + "\"" + "." + f + ", ";
				} else {
					result += tableName + "." + f + ", ";
				}
			}
			result = result.substring(0, result.lastIndexOf(','));
			rs.close();
			stmt.close();
		
			if (mappingDocument.getDatabaseType() == DatabaseType.POSTGRESQL || mappingDocument.getDatabaseType() == DatabaseType.ORACLE) {
				result += " FROM " + "\"" + tableName + "\"";
			} else {
				result += " FROM " + tableName;
			}
			result += " ORDER BY " + fields.get(0); //add order to ensure same order regardless the implementation
			
		} catch (Exception e) {
			log.error("Failed to create query for table " + tableName, e);
		} finally {
			try { stmt.close(); } catch (Exception e) {}
		}
		
		log.info("Result is: " + result);
		return result;
	}

	public void init() {
		log.info("Initializing.");
		
		//test source database
		db.openConnection();
		
		this.baseNs = properties.getProperty("default.namespace");
		String mappingFilename = properties.getProperty("mapping.file");
		
		InputStream isMap = FileManager.get().open(mappingFilename);
		mapModel = ModelFactory.createDefaultModel();
		try {
			mapModel.read(isMap, baseNs, properties.getProperty("mapping.file.type"));
		} catch (Exception e) {
			log.error("Error reading mapping file");
			System.exit(1);
		}
		//mapModel.write(System.out, properties.getProperty("mapping.file.type"));
		
		String inputModelFileName = properties.getProperty("input.model");
		
		Model resultBaseModel = ModelFactory.createDefaultModel();
		if (StringUtils.isNotBlank(inputModelFileName)) {
			InputStream isRes = FileManager.get().open(inputModelFileName);
			try {
				resultBaseModel.read(isRes, baseNs, properties.getProperty("input.model.type"));
			} catch (Exception e) {
				log.error("Error reading input model");
				System.exit(1);
			}
		}
		//resultBaseModel.write(System.out, properties.getProperty("input.model.type"));
		
		String storeInTdb = properties.getProperty("jena.storeOutputModelUsingTdb");
		String cleanTdbOnStartup = properties.getProperty("jena.cleanTdbOnStartup");
		
		verbose =  properties.containsKey("default.verbose") && properties.getProperty("default.verbose").contains("true");
			
		log.info("Initialising Parser");
		
		if (Boolean.valueOf(storeInTdb)) {
			if (Boolean.valueOf(cleanTdbOnStartup)) {
				Dataset dataset = TDBFactory.createDataset(properties.getProperty("jena.tdb.directory"));
				dataset.begin(ReadWrite.WRITE);
					Model m = dataset.getDefaultModel();
					m.removeAll();
					dataset.commit();
				dataset.end();
			}

			Dataset dataset = TDBFactory.createDataset(properties.getProperty("jena.tdb.directory"));
			dataset.begin(ReadWrite.READ);
				Model resultDbModel = dataset.getDefaultModel();
			dataset.end();
			
		    if (StringUtils.isNotBlank(inputModelFileName)) {
				InputStream isRes = FileManager.get().open(inputModelFileName);
				resultDbModel.read(isRes, baseNs, properties.getProperty("input.model.type"));
		    }
		    log.info("Store size is " + resultDbModel.size());
		    
			resultDbModel.add(resultBaseModel);
			
			resultModel = ModelFactory.createDefaultModel();

			Map<String, String> prefixes = mapModel.getNsPrefixMap();
			log.info("Copy " + prefixes.size() + " prefixes from map model to persistent.");
			for (String s : mapModel.getNsPrefixMap().keySet()) {
				log.info(s + ": " + mapModel.getNsPrefixMap().get(s));
			}
			resultModel.setNsPrefixes(prefixes);
			mappingDocument.setPrefixes(prefixes);
			
		} else {
			//resultModel = ModelFactory.createInfModel(ReasonerRegistry.getRDFSReasoner(), resultBaseModel);
			Map<String, String> prefixes = mapModel.getNsPrefixMap();
			prefixes.putAll(resultBaseModel.getNsPrefixMap());
			
			resultModel = ModelFactory.createDefaultModel();
			resultModel.setNsPrefixes(prefixes);
			resultModel.add(resultBaseModel);

			mappingDocument.setPrefixes(prefixes);
		}
	}
	
	boolean contains(LinkedList<LogicalTableMapping> logicalTableMappings, String uri) {
		for (LogicalTableMapping logicalTableMapping : logicalTableMappings) {
			if (StringUtils.equals(logicalTableMapping.getUri(), uri)) return true;
		}
		return false;
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
	
	public Model getResultModel() {
		return resultModel;
	}
	
	public Properties getProperties() {
		return properties;
	}

	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
}
