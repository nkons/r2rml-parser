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
package gr.ekt.r2rml.entities.sparql;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * A local resource, and associated information. Is contained in rows of a Sparql query resultset
 * @see LocalResultRow
 * @author nkons
 *
 */
public class LocalResource {
	private static final Logger log = LoggerFactory.getLogger(LocalResource.class);
	private Map<String, String> prefixMap;
	
	private String uri;
	private String uriEncoded;
	private String localName;
	private String namespace;
	private boolean resource = Boolean.FALSE;
	private boolean literal = Boolean.FALSE;
	private boolean blankNode = Boolean.FALSE;
	
	/**
	 * Create a local resource based on a jena model resource
	 */
	public LocalResource(Resource r) {
		//log.info("About to create " + r.getURI());
		if (r.isAnon()) {
			log.info("Found a blank node");
			this.blankNode = Boolean.TRUE;
			this.uri = null;
			this.uriEncoded = null;
			this.localName = r.getId().getLabelString();
			this.namespace = null;
		} else {
			//log.info("Node URI: " + r.getURI());
			this.prefixMap = r.getModel().getNsPrefixMap();

			if (r.isLiteral()) {
				this.literal = Boolean.TRUE;
				//log.info("Found a literal with URI: " + r.getURI());
			} else if (r.isResource()) {
				//log.info("Found a resource with URI: " + r.getURI());
				this.resource = Boolean.TRUE;
				this.uri = r.getURI();
				try {
					uriEncoded = URLEncoder.encode(r.getURI(), "UTF-8");
				} catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
				namespace = r.getNameSpace();
				
				if (namespace.contains("#")) {
					String prefix = findPrefixByUri(namespace);
					if (prefix == null) {
						this.localName = r.getURI();
					} else {
						this.localName = findPrefixByUri(namespace) + ":" + r.getLocalName();
					}
					
				} else {
					this.localName = r.getURI();
				}
				log.info("Local resource: " + localName);
			}
		}
	}
	
	/**
	 * Non-argument constructor.
	 */
	public LocalResource() {
	}
	
	/**
	 * Create a literal resource
	 */
	public LocalResource(Literal l) {
		this.literal = Boolean.TRUE;

		this.localName = l.getLexicalForm();
		//System.out.println("f from l" + localName);
	}
	
	public LocalResource(String s) {
		//log.info("Creating literal resource " + s);
		this.literal = Boolean.TRUE;
		
		this.localName = s;
	}
	
	public String findPrefixByUri(String uri) {
		if (!prefixMap.isEmpty()) {
			for (String prefix : prefixMap.keySet()) {
				if (prefixMap.get(prefix).equals(uri)) {
					return prefix;
				}
			}
			return null;
		} else {
			return null;
		}
	}
	
	/**
	 * @return the uri
	 */
	public String getUri() {
		return uri;
	}
	/**
	 * @param uri the uri to set
	 */
	public void setUri(String uri) {
		this.uri = uri;
	}
	/**
	 * @return the uriEncoded
	 */
	public String getUriEncoded() {
		return uriEncoded;
	}
	/**
	 * @param uriEncoded the uriEncoded to set
	 */
	public void setUriEncoded(String uriEncoded) {
		this.uriEncoded = uriEncoded;
	}
	/**
	 * @return the localName
	 */
	public String getLocalName() {
		return localName;
	}
	/**
	 * @param localName the localName to set
	 */
	public void setLocalName(String localName) {
		this.localName = localName;
	}
	/**
	 * @return the namespace
	 */
	public String getNamespace() {
		return namespace;
	}
	/**
	 * @param namespace the namespace to set
	 */
	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	/**
	 * @return the resource
	 */
	public boolean getResource() {
		return resource;
	}

	/**
	 * @param resource the resource to set
	 */
	public void setResource(boolean resource) {
		this.resource = resource;
	}

	/**
	 * @return the literal
	 */
	public boolean getLiteral() {
		return literal;
	}

	/**
	 * @param literal the literal to set
	 */
	public void setLiteral(boolean literal) {
		this.literal = literal;
	}

	/**
	 * @return the blankNode
	 */
	public boolean getBlankNode() {
		return blankNode;
	}

	/**
	 * @param blankNode the blankNode to set
	 */
	public void setBlankNode(boolean blankNode) {
		this.blankNode = blankNode;
	}
	
}
