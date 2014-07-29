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
package gr.seab.r2rml.entities;

import java.util.ArrayList;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * The template is a component of the form http://data.example.com/film/{film_id}. Encloses fields in brackets.
 * @see SubjectMap
 * @see PredicateObjectMap
 * @author nkons
 *
 */
public class Template {
	//private static final Logger log = LoggerFactory.getLogger(Template.class);
	/**
	 * The initial template text
	 */
	private String text;
	
	/**
	 * The language of the literals (if any) to be created.
	 */
	private String language;
	
	/**
	 * The fields, enclosed in brackets. Must correspond to database fields
	 */
	private ArrayList<String> fields;
	
	/**
	 * Returns literal, blank node or iri.
	 */
	private TermType termType;
	
	/**
	 * Is used as a prefix, in case of IRIs.
	 */
	private String namespace;

	/**
	 * The model.
	 */
	private Model model;
	
	public Template(String text, TermType termType, String namespace, Model model) {
		this.text = text;
		this.fields = createTemplateFields();
		this.termType = termType;
		this.namespace = namespace;
		this.model = model;
	}

	public Template(Literal literal, TermType termType, String namespace, Model model) {
		this.text = literal.getString();
		this.language = literal.getLanguage();
		this.fields = createTemplateFields();
		this.termType = termType;
		this.namespace = namespace;
		this.model = model;
	}
	
	public ArrayList<String> createTemplateFields() {
		ArrayList<String> results = new ArrayList<String>();
		String template = this.text;
		//log.info("\\{");
		template = template.replaceAll("\\\\\\{", " "); //remove escaped braces \{ that could mix things
		//log.info("new template " + template);
		while (template.indexOf('{') != -1) {
			int from = template.indexOf('{') + 1;
			int to = template.indexOf('}');
			results.add(template.substring(from, to));
			//log.info("adding variable '" + template.substring(from, to) + "'");
			template = template.substring(to + 1, template.length());
		}
		return results;
	}
	
	public boolean isUri() {
		String s = text;
		
		for (String key : model.getNsPrefixMap().keySet()) {
			//log.info("checking if " + s + " contains '" + key + ":' or " + model.getNsPrefixMap().get(key));
			if (s.contains(key + ":") || s.contains(model.getNsPrefixMap().get(key))) return true;
		}
		
		if (s.contains("http")) {
			return true;
		} else {
			return false;
		}
	}
	
	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}
	
	public String getLanguage() {
		return language;
	}
	
	public void setLanguage(String language) {
		this.language = language;
	}
	
	public ArrayList<String> getFields() {
		return fields;
	}

	public void setFields(ArrayList<String> fields) {
		this.fields = fields;
	}
	
	public TermType getTermType() {
		return termType;
	}
	
	public void setTermType(TermType termType) {
		this.termType = termType;
	}
	
	public String getNamespace() {
		return namespace;
	}
	
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}
	
	public Model getModel() {
		return model;
	}
	
	public void setModel(Model model) {
		this.model = model;
	}
		
}
