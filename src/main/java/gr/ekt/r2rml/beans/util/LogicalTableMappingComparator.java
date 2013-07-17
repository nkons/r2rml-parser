package gr.ekt.r2rml.beans.util;

import gr.ekt.r2rml.entities.LogicalTableMapping;
import gr.ekt.r2rml.entities.PredicateObjectMap;

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
