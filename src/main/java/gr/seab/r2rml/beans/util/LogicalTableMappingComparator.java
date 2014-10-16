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
package gr.seab.r2rml.beans.util;

import gr.seab.r2rml.entities.LogicalTableMapping;
import gr.seab.r2rml.entities.PredicateObjectMap;

import java.util.Comparator;

import org.apache.commons.lang.StringUtils;

public class LogicalTableMappingComparator implements Comparator<LogicalTableMapping> {

	public int compare(LogicalTableMapping logicalTableMapping0, LogicalTableMapping logicalTableMapping1) {
		
		for (PredicateObjectMap p : logicalTableMapping0.getPredicateObjectMaps()) {
			try {
				if (StringUtils.isNotBlank(p.getRefObjectMap().getParentTriplesMapUri())) return 1;
			} catch (Exception e) { }
		}
		
		for (PredicateObjectMap p : logicalTableMapping1.getPredicateObjectMaps()) {
			try {
				if (StringUtils.isNotBlank(p.getRefObjectMap().getParentTriplesMapUri())) return -1;
			} catch (Exception e) {	}
		}

		return 0;
	}
}
