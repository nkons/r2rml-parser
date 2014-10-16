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

import gr.seab.r2rml.entities.MappingDocument;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Calendar;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class Main {
	private static final Logger log = LoggerFactory.getLogger(Main.class);

	/**
	 * The properties, as read from the properties file.
	 */
	private static Properties properties = new Properties();
	
	public static void main(String[] args) {
		Calendar c0 = Calendar.getInstance();
        long t0 = c0.getTimeInMillis();
        
        CommandLineParser cmdParser = new PosixParser();

        Options cmdOptions = new Options();
        cmdOptions.addOption("p", "properties", true, "define the properties file. Example: r2rml-parser -p r2rml.properties");
        cmdOptions.addOption("h", "print help", false, "help");

		String propertiesFile = "r2rml.properties";
		
		try {
			CommandLine line = cmdParser.parse(cmdOptions, args);
			
			if (line.hasOption("h")) {
				HelpFormatter help = new HelpFormatter();
				help.printHelp("r2rml-parser\n", cmdOptions);
				System.exit(0);
			}

			if (line.hasOption("p")) {
				propertiesFile = line.getOptionValue("p");
			}
		} catch (ParseException e1) {
			//e1.printStackTrace();
			log.error("Error parsing command line arguments.");
			System.exit(1);
		}
		
		try {
			if (StringUtils.isNotEmpty(propertiesFile)) {
				properties.load(new FileInputStream(propertiesFile));
				log.info("Loaded properties from " + propertiesFile);
			}
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
			log.error("Properties file not found (" + propertiesFile + ").");
			System.exit(1);
		} catch (IOException e) {
			//e.printStackTrace();
			log.error("Error reading properties file (" + propertiesFile + ").");
			System.exit(1);
		}
		
		ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("app-context.xml");
		
		Database db = (Database) context.getBean("db");
		db.setProperties(properties);
		
		Parser parser = (Parser) context.getBean("parser");
		parser.setProperties(properties);
		
		MappingDocument mappingDocument = parser.parse();
		
		mappingDocument.getTimestamps().add(t0); //0 Started
		mappingDocument.getTimestamps().add(Calendar.getInstance().getTimeInMillis()); //1 Finished parsing. Starting generating result model.
		
		Generator generator = (Generator) context.getBean("generator");
		generator.setProperties(properties);
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
        for (int i = 0; i < mappingDocument.getTimestamps().size(); i++) {
        	if (i > 0) {
        		long l = (mappingDocument.getTimestamps().get(i).longValue() - mappingDocument.getTimestamps().get(i - 1).longValue());
        		//System.out.println(l);
        		log.info(String.valueOf(l));
        	}
        }
        log.info("Parse. Generate in memory. Dump to disk/database. Log. - Alltogether in " + String.valueOf(mappingDocument.getTimestamps().get(5).longValue() - mappingDocument.getTimestamps().get(0).longValue()) + " msec.");
        log.info("Done.");
        System.out.println("Done.");
	}
}
