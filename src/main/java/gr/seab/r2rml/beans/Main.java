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
package gr.seab.r2rml.beans;

import gr.seab.r2rml.entities.MappingDocument;

import java.io.File;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);

	public static void main(String[] args) {
		Calendar c0 = Calendar.getInstance();
        long t0 = c0.getTimeInMillis();
		String appContextFile = "app-context.xml";
		if (args.length > 0) {
			File f = new File(args[0]);
			if (f.exists()) {
				log.info("Spring context descriptor set to " + args[0]);
				appContextFile = args[0];
			} else {
				log.info("File " + args[0] + " not in classpath, using app-context.xml instead");
			}
		} else {
			log.info("Spring context file not provided, using app-context.xml");
		}
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(appContextFile);
		
		Parser parser = (Parser) context.getBean("parser");
		MappingDocument mappingDocument = parser.parse();
		
		mappingDocument.getTimestamps().add(t0); //0 Started
		mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //1 Finished parsing. Starting generating result model.
		
		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(parser.getProperties());
		generator.setResultModel(parser.getResultModel());
		
		//Actually do the output
		generator.createTriples(mappingDocument);
		
		context.close();
		Calendar c1 = Calendar.getInstance();
        long t1 = c1.getTimeInMillis();
        log.info("Finished in " + (t1 - t0) + " milliseconds. Done.");
		mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //5 Finished.
		//log.info("5 Finished.");

		//output the result
//        for (int i = 0; i < mappingDocument.getTimestamps().size(); i++) {
//        	if (i > 0) {
//        		System.out.println((mappingDocument.getTimestamps().get(i).longValue() - mappingDocument.getTimestamps().get(i - 1).longValue()));	
//        	}
//        }
//        System.out.println("Parse. Generate in memory. Dump to disk/database. Log. - Alltogether: " + String.valueOf(mappingDocument.getTimestamps().get(5).longValue() - mappingDocument.getTimestamps().get(0).longValue()));
        System.out.println("Done.");
	}
}
